from openagua_engine import create_app
from run import run_model

from dotenv import load_dotenv

load_dotenv()

app = create_app()


@app.task(name='model.run')
def run(**kwargs):
    network_id = kwargs.pop('network_id')
    scenario_id_combinations = kwargs.pop('scenario_ids', [])

    for scen_ids in scenario_id_combinations:
        # This is how to run a single scenario model asynchronously
        run_scenario.apply_async(args=(network_id, scen_ids,), kwargs=kwargs)


@app.task
def run_scenario(*args, **kwargs):
    # This is just a passthrough to the real model
    run_model(*args, **kwargs)
