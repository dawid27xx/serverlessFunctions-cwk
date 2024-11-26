# Stock Analysis Automation Workflow Using Azure Serverless Functions

This project is a part of the distributed systems module at the University of Leeds. We were tasked with creating a workflow of our choice using azure serverless functions.

The workflow consists of three functions designed to automate stock data processing. The first function retrieves up-to-date stock information and saves it to a database. The second calculates and stores simple moving averages (SMAs), which are widely used technical indicators for stock analysis. The third function provides on-demand access to these SMAs via an HTTP trigger, ensuring the workflow is both automated and user-interactive.
