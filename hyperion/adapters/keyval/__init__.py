"""Key-value store adapters. Import the concrete module you need explicitly.

``memory`` and ``filesystem`` are lite; ``dynamodb`` requires boto3 (``[aws]``).
No eager re-exports here -- touching this namespace must stay boto3-free.
"""
