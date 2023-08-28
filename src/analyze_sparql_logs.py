import argparse
import asyncio
import os
import re
import shutil
import sys
import time

import pandas as pd
import requests
from rdflib.plugins.sparql.algebra import (
    translateAlgebra,
    translateQuery,
)
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue

from tqdm.asyncio import tqdm_asyncio
from tqdm.asyncio import tqdm


class SparqlAnalysis:
    def __init__(self):
        self.df = None
        self.df_agg = None
        self.setPaths()

    def setPaths(self):
        argParser.parse_args()
        if os.environ["DATA_FOLDER"]:
            self.data_folder = os.environ["DATA_FOLDER"]
        else:
            self.data_folder = "./data"
        os.makedirs(self.data_folder, exist_ok=True)

        self.df_pkl_filename = f"{self.data_folder}/bio2rdf-sparql-log.df.pkl"
        self.df_agg_pkl_filename = f"{self.data_folder}/bio2rdf-sparql-log-agg.df.pkl"
        self.df_results_pkl_filename = (
            f"{self.data_folder}/bio2rdf-sparql-log-results.df.pkl"
        )

    def downloadRemoteFile(self, remote_file_path, local_folder, local_file_name):
        try:
            os.makedirs(local_folder, exist_ok=True)
            try:
                with requests.get(remote_file_path, stream=True) as r:
                    local_filename = f"{local_folder}/{local_file_name}"
                    with open(local_filename, "wb") as f:
                        shutil.copyfileobj(r.raw, f)
                    print(f"Downloaded {remote_file_path} to {local_filename}")
            except requests.exceptions.RequestException as e:
                r.raise_for_status()
                raise SystemExit(e)
            return True
        except Exception:
            print(f"Error in downloading {remote_file_path}")
            return False

    def getBio2RDFLogs(self):
        filename = "bio2rdf_sparql_logs_processed_01-2019_to_07-2021.csv"
        remote_file_path = f"https://download.dumontierlab.com/bio2rdf/logs/{filename}"

        if os.path.exists(self.df_pkl_filename) is True:
            s.df = pd.read_pickle(self.df_pkl_filename)
            return True

        print(f"Downloading {remote_file_path}")
        if (
            self.downloadRemoteFile(remote_file_path, self.data_folder, filename)
            is not True
        ):
            return False

        # read CSV into dataframe
        local_file = f"{self.data_folder}/{filename}"
        self.df = pd.read_csv(local_file, lineterminator="\n", dtype=str)
        self.df.columns = ["query", "domain", "agent", "timestamp"]

        # save as DF
        self.df.to_pickle(self.df_pkl_filename)
        print(f"saved {filename} as {self.df_pkl_filename}")

    def filter4SPARQLKeywords(self, text):
        keywords = ["select", "insert", "construct", "ask"]
        t = text.lower()
        for keyword in keywords:
            if t.find(keyword) != -1:
                return t
        return ""

    def removeHTTPparams(self, text):
        # remove the HTTP parameters that are incorrectly included from the source data file
        params = [
            "&format",
            "&timeout",
            "&debug",
            "&run",
            "&maxrows",
            "&infer",
            "&output",
            "&results",
            "&default-graph-uri",
        ]

        # Regular expression to find any of the keywords in the query
        regex = "|".join(re.escape(param) for param in params)

        # Remove the matched part of the query and everything after it
        result = re.sub(rf"({regex}).*", "", text)
        return result

    def aggregateQueries(self):
        # aggregate the queries and get their frequency
        if os.path.exists(self.df_agg_pkl_filename) is True:
            s.df_agg = pd.read_pickle(self.df_agg_pkl_filename)
            return True

        tqdm.pandas()
        # self.df['query'] = self.df['query'].swifter.apply(self.filter4SPARQLKeywords)
        self.df["query"] = self.df["query"].swifter.apply(self.removeHTTPparams)

        self.df_agg = self.df.groupby("query", as_index=False).size()

        self.df_agg.to_pickle(self.df_agg_pkl_filename)
        print(f"saved aggregate as {self.df_agg_pkl_filename}")

        return True

    def pAlgebra(q) -> None:
        def pp(p, ind="    "):
            if not isinstance(p, CompValue):
                print(p)
                return
            print("%s(" % (p.name,))
            for k in p:
                print(
                    "%s%s ="
                    % (
                        ind,
                        k,
                    ),
                    end=" ",
                )
                pp(p[k], ind + "    ")
            print("%s)" % ind)

        try:
            pp(q.algebra)
        except AttributeError:
            # it's update, just a list
            for x in q:
                pp(x)

    async def parseSPARQLquery(self, query):
        try:
            stdout = sys.stdout
            sys.stdout = open(os.devnull, "w")

            parsed = parseQuery(query)
            algebra = translateQuery(parsed)
            # pprintAlgebra(algebra)
            translated = translateAlgebra(algebra)

            sys.stdout = stdout
            return query, algebra, translated
        except Exception:
            return query, "", ""

    async def parseSPARQLquery2(self, query, asyncio_semaphore):
        async with asyncio_semaphore:
            try:
                stdout = sys.stdout
                sys.stdout = open(os.devnull, "w")

                parsed = parseQuery(query)
                algebra = translateQuery(parsed)
                # pprintAlgebra(algebra)
                translated = translateAlgebra(algebra)

                sys.stdout = stdout
                return query, algebra, translated
            except Exception:
                return query, "", ""

    async def process_all_queries(self):
        if self.df_agg is None:
            if os.path.exists(self.df_agg_pkl_filename) is True:
                self.df_agg = pd.read_pickle(self.df_agg_pkl_filename)

        df = self.df_agg
        # df = self.df_agg[:2000].copy()
        # stdout = sys.stdout
        # sys.stdout = open(os.devnull, 'w')

        queries = df["query"].values.tolist()

        tasks = [self.parseSPARQLquery(query) for query in queries]
        results = await tqdm_asyncio.gather(*tasks)

        # asyncio_semaphore = asyncio.BoundedSemaphore(100)
        # tasks = []
        # for query in queries:
        #     #task = asyncio.create_task(self.parseSPARQLquery(query))
        #     task = asyncio.ensure_future(self.parseSPARQLquery2(query, asyncio_semaphore))
        #     tasks.append(task)

        # results = [
        #     await f
        #     async for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))
        # ]

        # for task in tqdm_asyncio.as_completed(tasks):
        #     # get the next result
        #     result = await task
        #     #print(f'>got {result}')

        # pbar = tqdm(total=len(tasks))
        # results = []
        # for f in asyncio.as_completed(tasks):
        #     result = await f
        #     results.append(result)
        #     #pbar.set_description(value)
        #     pbar.set_description("parsing", refresh=True)
        #     # lquery.append(value[0])
        #     # lalgebra.append(value[1])
        #     # ltranslated.append(value[2])
        #     pbar.update()

        df = pd.DataFrame(results, columns=["query", "algebra", "translated"])
        df.to_pickle(self.df_results_pkl_filename)

        return True


if __name__ == "__main__":
    project = "sparql-logs"

    startTime = time.time()
    argParser = argparse.ArgumentParser(
        prog="SPARQL Log Analysis Tool",
        description="For the analysis of SPARQL logs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    argParser.add_argument(
        "-w", "--dir", default="./data", help="Directory to download files into"
    )

    s = SparqlAnalysis()
    s.getBio2RDFLogs()
    s.aggregateQueries()
    asyncio.run(s.process_all_queries())

    executionTime = time.time() - startTime
    print(f"Execution time: {executionTime} seconds")
