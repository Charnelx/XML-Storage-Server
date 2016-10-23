XML_Server

Small Flask-based server for storing XML files with partial REST API support

This app supports upload and download of XML files separated by owner (organization).
Partially implemented REST API for operating with documents (add, get, delete).

Requirements:
 * MongoDB running locally (v. 3.0.7 tested)
 * pymongo 3.0.3
 * Flask 0.11.1
 * Flask_login 0.3.2
 * xmljson 0.1.7

How to use:
 * Initialize DB with db_init.py
 * Run app.py
 * Run test_app.py