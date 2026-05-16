"""Schema registry adapters. Import the concrete module you need explicitly.

``local`` is lite; ``s3`` requires boto3 (``[aws]``).
No eager re-exports here -- touching this namespace must stay boto3-free.
"""
