"""Secrets manager adapters. Import the concrete module you need explicitly.

``env`` and ``dummy`` are lite; ``aws_sm`` requires boto3 (``[aws]``).
No eager re-exports here -- touching this namespace must stay boto3-free.
"""
