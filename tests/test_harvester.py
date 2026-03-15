# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

import httpx
import pytest
import respx

from coreason_etl_euctr.harvester import EpistemicHarvesterTask


@pytest.fixture
def harvester() -> EpistemicHarvesterTask:
    client = httpx.Client()
    # Set rate_limit to 0 for tests to avoid delays
    return EpistemicHarvesterTask(client=client, rate_limit=0)


def test_extract_ids_from_html_valid(harvester: EpistemicHarvesterTask) -> None:
    html = """
    <div>
        <span class="label">EudraCT Number:</span> 2008-006649-18
        <br>
        <span class="label">EudraCT Number:</span> 2008-002768-32
        <br>
        <span class="label">EudraCT Number:</span> 2008-006649-18 <!-- duplicate -->
        <span class="label">Other Number:</span> 12345
    </div>
    """
    ids = harvester.extract_ids_from_html(html)
    assert ids == ["2008-006649-18", "2008-002768-32"]


def test_extract_ids_from_html_empty(harvester: EpistemicHarvesterTask) -> None:
    html = "<div><p>No results found</p></div>"
    ids = harvester.extract_ids_from_html(html)
    assert ids == []


def test_extract_ids_from_html_no_sibling(harvester: EpistemicHarvesterTask) -> None:
    html = '<div><span class="label">EudraCT Number:</span></div>'
    ids = harvester.extract_ids_from_html(html)
    assert ids == []


@respx.mock
def test_harvest_pagination(harvester: EpistemicHarvesterTask) -> None:
    # Page 1 mock
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=1").respond(
        status_code=200,
        html='<span class="label">EudraCT Number:</span> 2010-022400-53',
    )
    # Page 2 mock
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=2").respond(
        status_code=200,
        html='<span class="label">EudraCT Number:</span> 2008-003174-18',
    )
    # Page 3 mock (empty results stops pagination)
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=3").respond(
        status_code=200, html="<div>No results</div>"
    )

    result = harvester.harvest(max_pages=5)

    # Must be sorted
    assert result == ["2008-003174-18", "2010-022400-53"]


@respx.mock
def test_harvest_with_date_from(harvester: EpistemicHarvesterTask) -> None:
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=1&dateFrom=2024-01-01").respond(
        status_code=200,
        html='<span class="label">EudraCT Number:</span> 2006-001095-21',
    )
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=2&dateFrom=2024-01-01").respond(
        status_code=200, html="<div>No results</div>"
    )

    result = harvester.harvest(date_from="2024-01-01", max_pages=3)
    assert result == ["2006-001095-21"]


@respx.mock
def test_harvest_http_error(harvester: EpistemicHarvesterTask) -> None:
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=1").respond(status_code=500)

    # Should break and return empty rather than raising exception or looping
    result = harvester.harvest(max_pages=2)
    assert result == []


@respx.mock
def test_harvest_max_pages_reached(harvester: EpistemicHarvesterTask) -> None:
    # Both page 1 and page 2 return results
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=1").respond(
        status_code=200,
        html='<span class="label">EudraCT Number:</span> 1111-111111-11',
    )
    respx.get("https://www.clinicaltrialsregister.eu/ctr-search/search?query=&page=2").respond(
        status_code=200,
        html='<span class="label">EudraCT Number:</span> 2222-222222-22',
    )

    # Even if page 2 has results, max_pages=2 should stop it there
    result = harvester.harvest(max_pages=2)
    assert result == ["1111-111111-11", "2222-222222-22"]
