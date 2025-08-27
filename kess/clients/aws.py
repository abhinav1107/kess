from typing import Dict, Optional, Any

import boto3
import base64
import time
from botocore.exceptions import ClientError, NoCredentialsError
from kess.utils.log_setup import get_logger, with_context


class AWSClient:
    """AWS Client for interacting with AWS services."""
    def __init__(self, credentials: Optional[Dict[str, str]] = None):
        """
        Initialize the AWS client with optional credentials.
        :param credentials: Optional dict with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
        """
        self._ctx = with_context(get_logger("aws_client"), component="aws_client")
        self._credentials = credentials

        if credentials:
            self._ctx.debug("Using provided AWS credentials.")
            self.session = boto3.Session(
                aws_access_key_id=credentials.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=credentials.get("AWS_SECRET_ACCESS_KEY")
            )
        else:
            self._ctx.debug("Using default AWS credentials.")
            self.session = boto3.Session()

    def validate_ecr_credentials(self) -> bool:
        """Validate that AWS credentials can access ECR."""
        try:
            ecr_client = self.session.client('ecr')

            # Try a simple operation
            ecr_client.describe_registries()
            self._ctx.info("AWS ECR credentials validated successfully")
            return True

        except ClientError as e:
            self._ctx.error(f"AWS ECR validation failed: {e}")
            return False
        except NoCredentialsError:
            self._ctx.error("No AWS credentials found")
            return False
        except Exception as e:
            self._ctx.error(f"Unexpected error validating ECR credentials: {e}")
            return False

    def _parse_ecr_response(self, auth_data: Dict[str, Any], server: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Parse ECR authorization response."""
        try:
            auth_token = auth_data.get('authorizationToken')
            if not auth_token:
                self._ctx.error("No authorization token in response")
                return None

            # Decode base64 token to get username:password
            decoded_token = base64.b64decode(auth_token).decode('utf-8')
            username, password = decoded_token.split(':', 1)

            if server:
                endpoint = server
            else:
                endpoint = auth_data.get('proxyEndpoint', '')
                if not endpoint:
                    self._ctx.error("No proxy endpoint in response")
                    return None

            result = {
                'username': username,
                'password': password,
                'server': endpoint,
                'timestamp': int(time.time())
            }

            self._ctx.info(f"Successfully parsed ECR token for {endpoint}")
            return result

        except Exception as e:
            self._ctx.error(f"Failed to parse ECR response: {e}")
            return None

    def _get_default_ecr_token(self) -> Optional[Dict[str, Any]]:
        """Get ECR token using default region detection."""
        try:
            ecr_client = self.session.client('ecr')
            response = ecr_client.get_authorization_token()

            if not response.get('authorizationData'):
                self._ctx.error("No authorization data in ECR response")
                return None

            auth_data = response['authorizationData'][0]

            return self._parse_ecr_response(auth_data)

        except Exception as e:
            self._ctx.error(f"Failed to get default ECR token: {e}")
            return None

    def _get_specific_ecr_token(self, ecr_url: str) -> Optional[Dict[str, Any]]:
        try:
            parts = ecr_url.split('.')
            if len(parts) < 4:
                self._ctx.error(f"Invalid ECR URL format: {ecr_url}")
                return None

            region = parts[3]
            self._ctx.info(f"Using ECR region: {region}")

            # Create ECR client for specific region
            ecr_client = self.session.client('ecr', region_name=region)
            response = ecr_client.get_authorization_token()
            if not response.get('authorizationData'):
                self._ctx.error("No authorization data in ECR response")
                return None

            auth_data = response['authorizationData'][0]

            return self._parse_ecr_response(auth_data, ecr_url)

        except Exception as e:
            self._ctx.error(f"Failed to get ECR token for {ecr_url}: {e}")
            return None

    def get_ecr_token(self, ecr_url: str) -> Optional[Dict[str, Any]]:
        """
        Get ECR authentication token.

        Args:
            ecr_url: ECR registry URL or 'default' for auto-detection

        Returns:
            Dict with username, password, server, and timestamp, or None if failed
        """
        try:
            if ecr_url == "default":
                return self._get_default_ecr_token()
            else:
                return self._get_specific_ecr_token(ecr_url)

        except Exception as e:
            self._ctx.error(f"Failed to get ECR token for {ecr_url}: {e}")
            return None
