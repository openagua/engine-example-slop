# SLOPPY

This is a proof-of-concept example of how to connect a custom model to OpenAgua for controlling from the OpenAgua web application using Python and the [openagua-engine](https://pypi.org/project/openagua-engine/) Python package.

## Overview

This example applies the Standard Linear Operating Policy (SLOP) to a simple network with an inflow, reservoir, agricultural diversion from the reservoir, instream flow requirement, and outflow. The example, called SLOPPY to emphasize it's proof-of-concept stage (and of course to reflect the main model's filename, *slop.py*), uses OpenAgua's task queue system and is designed to run continuously, waiting for model runs to be started by the web app user.

The workflow and associated files are as follows:

1. The SLOP model itself (*slop.py*). This is a Python class (SloppyModel) that:
   1. initializes by using the OpenAgua API client* to read in the network and input data (system configuration, capacities, inflow data, etc.) from the OpenAgua database
   2. on a per-step basis, calculates how much water should be delivered to the agricultural and instream demands (or spilled)

2. A generalized run routine (*run.py*) that:
   1. creates the openagua-engine instance (called "oa")
   2. creates the SloppyModel with relevant parameters (the OpenAgua network ID, scenario ID, etc.)
   3. progresses step-wise through the model timesteps, intermittently reporting progress to the app user (`oa.step(...)`)
   4. calls the model to save the data to the OpenAgua database once the model is finished (or encounters an error)
   5. finally, reports that everything is finished

3. A routine (*tasks.py*) that provides two asynchronous tasks for the task queue manager to handle when waiting for a new model to be run (i.e. after the user runs a model from the app)
   1. a task that awaits a new item in the queue, which may include multiple scenarios, and
   2. a task that calls a scenario-specific run routine

4. A routine that starts the application to wait for tasks.

*The OpenAgua client is a separate Python package called [openagua-client](https://pypi.org/project/openagua-client/) and is already included in the openagua-engine package as a "Client" attribute of the engine.

**The first task parses scenario information into one or more specific model runs, which may be run asynchronously and in parallel.

## Installation and use

Note these assume a working knowledge of Python and hence leaves out some detail (environment setup, etc.)

To use this example:
1. Obtain the necessary keys (see the [openagua-engine configuration instructions](https://github.com/openagua/openagua-engine#configuration)) and save them to a .env file in the project's root directory.
2. Install the dependancies (`pip install -r requirements.txt`)
3. Run *app.py* (`python app.py`)

For debugging without waiting for a task command from the queue, the *run.py* file can be used.