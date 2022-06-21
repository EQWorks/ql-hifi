import time
import statistics

from memory_profiler import memory_usage

from modules import duck


def sort_local(fpath):
    def profilable():
        fname = fpath.split('/')[-1].split('.')[0]
        with duck.engine.connect() as conn:
            conn.execute(
                f'''
                    COPY (
                        SELECT *
                        FROM read_csv_auto('{fpath}', header=True)
                        ORDER BY 1
                    ) TO 'data/{fname}_local_sorted.csv' WITH (HEADER 1);
                '''
            )

    return profilable


def sort_remote_gz(s3_uri):
    def profilable():
        duck.init()  # init the database with S3 connectivity
        fname = s3_uri.split('/')[-1].split('.')[0]
        with duck.engine.connect() as conn:
            conn.execute(
                f'''
                    COPY (
                        SELECT *
                        FROM read_csv_auto('{s3_uri}', header=True)
                        ORDER BY 1
                    ) TO 'data/{fname}_remote_sorted.csv' WITH (HEADER 1);
                '''
            )

    return profilable


def profile(path):
    if path.startswith('s3://'):
        profilable = sort_remote_gz(path)
    else:
        profilable = sort_local(path)

    print('-' * 80)
    start = time.time()
    mu = memory_usage(profilable)
    print(f'Min memory usage: {min(mu)}')
    print(f'Mean memory usage: {statistics.mean(mu)}')
    print(f'Median memory usage: {statistics.median(mu)}')
    print(f'Max memory usage: {max(mu)}')
    print(f'Sort {path} time usage: {time.time() - start}')
    print('-' * 80)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="A poor-person's profiler for DuckDB sort"
    )
    parser.add_argument('path', help='Path to the file to sort')  # positional argument
    parser.add_argument(
        '--max-memory', help='Max memory usage in MB', type=int, default=None
    )
    args = parser.parse_args()

    if args.max_memory:
        config = f"SET max_memory='{args.max_memory}MB'"
        with duck.engine.connect() as conn:
            conn.execute(config)

        print(config)

    profile(args.path)
