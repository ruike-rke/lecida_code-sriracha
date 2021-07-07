# Copyright (C) 2019 Lecida Inc
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of
# Lecida Inc. The intellectual and technical concepts contained herein are
# proprietary to Lecida Inc and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination or reproduction of this material is strictly forbidden unless
# prior written permission is obtained from Lecida Inc.
"""CircleCI CLI utilities."""

from typing import Any, Dict, Optional

import circleci.api

CIRCLECI_USERNAME = 'lecida'
CIRCLECI_VCS_TYPE = 'github'


def trigger_job(api_token: str, project: str, branch: str, job: str,
                revision: Optional[str], tag: Optional[str]) -> Dict[str, Any]:
    """Trigger a CircleCI build.

    Args:
        api_token: The CircleCI API token.
        project: The case sensitive repo name.
        branch: The branch to build.
        job: The name of the job to run.
        revision: The specific git revision to build. If None, the head of the
            branch is used. Can not be used with the tag parameter.
        tag: The git tag to build. Cannot be used with the revision parameter.

    """
    params = {'CIRCLE_JOB': job}

    api = circleci.api.Api(token=api_token)
    return api.trigger_build(username=CIRCLECI_USERNAME, project=project,
                             branch=branch, revision=revision, tag=tag,
                             params=params, vcs_type=CIRCLECI_VCS_TYPE)
