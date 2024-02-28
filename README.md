# Monte Carlo Example - Acryl DataHub Python SDK
This repository provides examples of specific integrations to the [Acryl DataHub](https://datahubproject.io/docs) using Acryl's Python SDK. The Acryl 
DataHub is a serverless, scalable platform for data processing and integration that allows developers to build powerful applications quickly and 
easily. 

## Getting Started
To get started with the provided examples, you need to have Python installed on your machine along with virtual environment module (venv). Here 
are the steps to run the Monte Carlo example from this repository:

1. Create a new virtual environment:
```bash
python3 -m venv venv
```

2. Activate the virtual environment:
- On Linux/macOS: `source venv/bin/activate`
- On Windows: `.\venv\Scripts\Activate.ps1`

3. Install the necessary packages using pip:
```bash
pip install -r requirements.txt
```

4. Run the Monte Carlo example:
```bash
python ./examples/monte_carlo_example.py
```

## Requirements
To run the examples, you'll need to have the following prerequisites installed on your system:
- Python 3.7 or later
- Virtual Environment Module (venv)

## Repository Structure
This repository contains several subdirectories for different example scenarios:
```
.
├── README.md       <-- This file
├── examples        <-- Contains all the Python code samples
│    ├── monte_carlo_example.py   <-- Monte Carlo sample code
|   └── ...
├── requirements.txt  <-- Lists all dependencies for this project
├── setup.py         <-- Setup script to package this project
└── src              <-- Contains the main Python package that provides classes and functions
    └── acryl_datahub_client   <-- Acryl DataHub Python SDK
```

## Additional Resources
For more information on specific integrations to the Acryl DataHub using Python SDK, you can refer to the [Acryl DataHub API 
documentation](https://datahubproject.io/docs/api/datahub-apis#datahub-api-comparison). 

If you have any questions or need assistance, please don't hesitate to reach out on Datahub community slack. Happy coding!