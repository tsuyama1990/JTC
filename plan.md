1. **Fix `pyproject.toml` Pytest Markers**:
   - Remove `[tool.pytest.ini_options.markers]` section.
   - Add `markers = ["live: marks tests as live API tests"]` under `[tool.pytest.ini_options]`.
2. **Fix `tests/e2e/test_live_pipeline.py` Skip Condition**:
   - Check if `settings.JQUANTS_REFRESH_TOKEN` is empty, or a known dummy value (e.g. `"test_token"`, `"dummy"`).
   - If it is, explicitly call `pytest.skip()` rather than proceeding and failing.
3. **Pre-commit Steps**:
   - Run `pre_commit_instructions` and make sure testing, verifications, reviews and reflections are done.
   - Run tests and linting to ensure everything passes locally.
4. **Submit**:
   - Submit the changes to fix the broken live test.
