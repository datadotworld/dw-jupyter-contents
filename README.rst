==========
dwcontents
==========

A Jupyter content provider for data.world.

This content provider allows data.world users to store and manage their notebooks and files
directly on data.world using Jupyter Notebook or Jupyter Lab.

Once enabled, the content provider will allow you to browse and access your entire data.world
library, including datasets and projects that you have created, contribute to or have bookmarked.


Quick start
===========

Install
-------

You can install it using ``pip`` directly from PyPI::

    pip install dwcontents


Configure
---------

Find or create a file named ``jupyter_notebook_config.py`` under your Jupyter folder (``~/.jupyter``).

Update it to define two configuration parameters:
- ``NotebookApp.contents_manager_class``: Must be set to ``dwcontents.DwContents``
- ``DwContents.dw_auth_token``: Must be your data.world API token (obtained at https://data.world/settings/advanced)

For example:

.. code-block:: python

    import dwcontents
    c = get_config()
    c.NotebookApp.contents_manager_class = dwcontents.DwContents
    c.DwContents.dw_auth_token = 'YOUR TOKEN GOES HERE'


Run
---

Once installation and configuration are complete, run Jupyter Notebook or Labs like you normally would.

For example::

    jupyter notebook

Known Issues
------------

- Jupyter supports a wide variety of file operations, whereas support for directories on data.world is limited.
  For a better experience, try to keep a flat file structure under your datasets and projects.