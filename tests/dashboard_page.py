"""Helper script for tests/test_dashboard.py: runs dashboard/app.py with only the page named in DF_PAGE."""
import os
import runpy
from pathlib import Path

import streamlit as st

_navigation = st.navigation


def _single_page(pages, **kwargs):
    return _navigation([p for p in pages if p.title == os.environ["DF_PAGE"]], **kwargs)


st.navigation = _single_page
runpy.run_path(str(Path(__file__).resolve().parent.parent / "dashboard" / "app.py"), run_name="__main__")
