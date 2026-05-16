"""Message queue adapters. Import the concrete module you need explicitly.

``memory`` and ``filesystem`` are lite; ``sqs`` requires boto3 (``[aws]``).
No eager re-exports here -- touching this namespace must stay boto3-free.
"""
