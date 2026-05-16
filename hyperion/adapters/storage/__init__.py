"""Storage adapters. Import the concrete module you need explicitly.

``memory`` and ``filesystem`` are lite (stdlib only); ``s3`` requires boto3.
No eager re-exports here -- touching this namespace must stay boto3-free.
"""
