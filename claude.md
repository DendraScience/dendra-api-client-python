# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
dendra_api_client_python is a helper library that places python wrapper functions around REST API calls to the Dendra APIv2 interface.  

The goal of this claude-code effort is to simplify the code, clean it up, and replace any redundant functions from the REST API.  

https://api-v2-docs.dendra.science/ provides API docs for the Dendra v2 API.  

## Commands
Clean up and update any older python code or syntax

Simplify existing functions if possible.  

Add a boolean argument for authentication, needs_auth, default false, to all functions which already use authentication.  

Add unit tests for any functions that do not already have unit tests.

Update the Jupyter Notebook example if there has been any changes to the arguments.  

 
