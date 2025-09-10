"""
S3 utility functions for listing and exploring S3-compatible storage.
"""

import datetime


def ls_s3_with_date(spark, path: str):
    """Utility to list files with modification dates under an S3 path."""
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI.create(path),
        spark._jsc.hadoopConfiguration()
    )
    for s in fs.listStatus(jvm.org.apache.hadoop.fs.Path(path)):
        dt = datetime.datetime.fromtimestamp(s.getModificationTime()/1000.0)
        print(f"{dt:%Y-%m-%d %H:%M:%S}  {s.getPath()}")


def ls_s3_recursive(spark, path: str, only_ext: str | None = None):
    """Recursively list files with modification dates under an S3 path."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(path), conf)
    Path = jvm.org.apache.hadoop.fs.Path

    stack = [Path(path)]
    while stack:
        p = stack.pop()
        for s in fs.listStatus(p):
            if s.isDirectory():
                stack.append(s.getPath())
            else:
                name = s.getPath().getName()
                if only_ext and not name.endswith(only_ext):
                    continue
                dt = datetime.datetime.fromtimestamp(s.getModificationTime()/1000.0)
                print(f"{dt:%Y-%m-%d %H:%M:%S}  {s.getPath()}")
