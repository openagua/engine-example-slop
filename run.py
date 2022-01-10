import os
from sys import stderr
from time import sleep
import datetime as dt
from loguru import logger
from openagua_engine import OpenAguaEngine
from slop import SloppyModel


def run_model(network_id, scenario_ids, **kwargs):
    """
    This is the main model run. The example is not built out at all. It just shows a skeleton, with usage of
    openagua-engine (OpenAguaEngine) and it's reporting facilities (start, step, stop, etc.)
    :param network_id: The network ID (this could also be changed to pass the entire network, to save time).
    :param scenario_ids: The scenario ID combinations for this run.
    :param kwargs: As-yet-undefined kwargs
    :return:
    """

    guid = kwargs.get('guid')

    run_name = kwargs.get('run_name')
    debug = kwargs.get('debug')
    logger.remove()
    loglevel = os.environ.get('LOGURU_LEVEL', 'INFO')
    logger.add(stderr, level=loglevel)

    logger.info('Running model with scenarios: '.format(scenario_ids))

    oa = OpenAguaEngine(
        name=run_name,
        guid=guid,
        network_id=network_id,
        run_key=None,  # basic run
        scenario_ids=scenario_ids,
    )

    # Tell OA that the model is started (this reports to the OpenAgua API and any logged in web client)
    oa.start()
    logger.info('Started')

    # Get the network data (note the ['network'] at the end; this will be fixed in the future).
    network = oa.Client.get_network(network_id)['network']
    template_id = network['layout']['active_template_id']
    template = oa.Client.get_template(template_id)['template']

    # create the model
    model = SloppyModel(oa.Client, network, template, scenario_ids, run_name)

    oa.total_steps = model.total_steps  # This lets the engine report the correct percent complete

    i = 0

    for i, date in enumerate(model.dates):

        # Check if the user has paused or stopped the run
        if oa.paused:
            pause_start_time = dt.datetime.now()
            while oa.paused and (dt.datetime.now() - pause_start_time).seconds < 86400:
                sleep(0.1)

        if oa.stopped:  # this should be after pause is checked, to stop during a pause
            oa.stop()  # Tell OA that the model is stopped
            break

        try:
            # Run the model one step
            model.step()

            if date.month == 10 and date.day == 1:
                logger.info(date)

            # This reports progress to the web client (i.e. app user)
            if date.day == 1:
                oa.step(datetime=date, step=i + 1)

        except Exception as err:
            logger.info(err)
            oa.error(extra_info=err)
            break

    oa.step(datetime=date, step=i + 1)
    logger.debug(f'Model finished after {i + 1} steps. Saving...')
    model.save()
    logger.debug('Data saved!')

    # Tell OA that the model is finished
    oa.finish(step=i + 1)  # include the last step in case we are only intermittently reporting


if __name__ == '__main__':
    import dotenv

    dotenv.load_dotenv()

    network_id = 1548
    # all_scenario_ids = [[3610], [3682]]
    all_scenario_ids = [[3610]]
    kwargs = dict(
        run_name='baseline',
        request_host='http://localhost:5000',
        guid='test-model',
        debug=True
    )

    for scenario_ids in all_scenario_ids:
        run_model(network_id, scenario_ids=scenario_ids, **kwargs)

    logger.info('Finished')
