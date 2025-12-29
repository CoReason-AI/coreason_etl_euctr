"""
Test PMDA Scaffolding.
"""


def test_pmda_import() -> None:
    """Test that the PMDA package can be imported."""
    import coreason_etl_pmda

    assert coreason_etl_pmda is not None


def test_pmda_modules_import() -> None:
    """Test that PMDA modules can be imported."""
    from coreason_etl_pmda import crawler, downloader, models, parser, pipeline

    assert crawler is not None
    assert downloader is not None
    assert parser is not None
    assert models is not None
    assert pipeline is not None
