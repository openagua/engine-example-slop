import datetime as dt
from ast import literal_eval
import pandas as pd
from dateutil.parser import parse
from loguru import logger

class SloppyModel(object):

    def __init__(self, oa_client, network, template, scenario_ids, run_name, debug=False):
        baseline_scenarios = [s for s in network['scenarios'] if s['layout']['class'] == 'baseline']
        baseline_scenario = baseline_scenarios[0]
        self.run_name = run_name
        start = parse(baseline_scenario['start_time'])
        end = parse(baseline_scenario['end_time'])
        timestep = baseline_scenario['time_step']
        freq = {'day': 'D', 'month': 'M'}.get(timestep)
        dates = pd.date_range(start, end, freq=freq)

        self.network = network
        self.template = template
        self.dates = dates[:100] if debug else dates
        self.total_steps = len(self.dates)
        self.i = -1

        self.conn = oa_client

        self.resources = {}

        self._init(network, scenario_ids)

        self.results = pd.DataFrame(
            index=pd.DatetimeIndex(self.dates),
            columns=[
                'Reservoir/Storage',
                'Agricultural Demand/Delivery',
                'Instream Demand/Delivery',
                'Outflow/Flow',
            ])

    @staticmethod
    def get_node_by_type(nodes, template_id, type_name):
        for node in nodes:
            _type_nodes = [t for t in node['types'] if t['name'] == type_name and t['template_id'] == template_id]
            if _type_nodes:
                return node

    def _init(self, network, scenario_ids):
        nodes = network['nodes']
        template_id = network['layout']['active_template_id']

        # pull out the resource scenarios (data), overwriting parent scenarios with child scenarios
        self.attr_data = {}
        scenarios = {}
        scenario_names = []
        baseline = [s for s in network['scenarios'] if s['layout']['class'] == 'baseline'][0]
        organized_scenario_ids = [baseline['id']]

        logger.info('Collecting data')
        for i, scenario_id in enumerate(scenario_ids):

            scenario = [s for s in network['scenarios'] if s['id'] == scenario_id][0]
            logger.info(f"Processing scenario \"{scenario['name']}\"")

            # create the final scenario name
            scenario_names.append(scenario['name'])
            _scenario_id = scenario_id
            branch_scenario_ids = []

            # loop through the scenario path (branch)
            while _scenario_id and _scenario_id not in scenarios:
                if _scenario_id not in organized_scenario_ids:
                    branch_scenario_ids.append(_scenario_id)
                _scenario = self.conn.get_scenario(_scenario_id, include_data=True)['scenario']
                logger.debug('Collecting data for scenario "{}"'.format(_scenario['name']))
                scenarios[_scenario_id] = _scenario
                _scenario_id = _scenario['parent_id']

            organized_scenario_ids.extend(list(reversed(branch_scenario_ids)))

        logger.info('Organizing data')
        for i, scenario_id in enumerate(organized_scenario_ids):
            scenario = scenarios[scenario_id]
            logger.debug('Overriding data with scenario "{}"'.format(scenario['name']))
            for resource_scenario in scenario['resourcescenarios']:
                res_attr_id = resource_scenario['resource_attr_id']
                dataset = resource_scenario['dataset']
                if res_attr_id not in self.attr_data or self.attr_data[res_attr_id] != dataset:
                    self.attr_data[res_attr_id] = dataset

        # The scenario name needs to be defined for this uncertainty/option combination
        # For now, this is defined here, rather than provided by the GUI or OpenAgua Engine
        self.scenario_name = '{}: {}'.format(self.run_name, ' / '.join(scenario_names))

        # inflow
        node = self.get_node_by_type(nodes, template_id, 'Inflow')
        self.resources['Inflow'] = node
        inflow = self.get_attr_value(node, 'Runoff')
        self.data = inflow * 0.0864
        self.data.columns = ['inflow']

        # ag demand
        node = self.get_node_by_type(nodes, template_id, 'Agricultural Demand')
        self.resources['Agricultural Demand'] = node
        demand = self.get_attr_value(node, 'Demand')
        self.data['demand'] = demand * 0.0864

        # instream demand
        node = self.get_node_by_type(nodes, template_id, 'Instream Demand')
        self.resources['Instream Demand'] = node
        demand = self.get_attr_value(node, 'Instream Flow Requirement')
        self.data['ifr'] = demand * 0.0864

        node = self.get_node_by_type(nodes, template_id, 'Reservoir')
        self.resources['Reservoir'] = node
        self.reservoir_capacity = self.get_attr_value(node, 'Storage Capacity')
        self.initial_storage = self.get_attr_value(node, 'Initial Storage')

        self.resources['Outflow'] = self.get_node_by_type(nodes, template_id, 'Outflow')

        logger.info(f'Running scenario \"{self.scenario_name}\"')


        return

    def get_attr_value(self, res, attr_name):
        res_attr_id = next((ra for ra in res['attributes'] if ra['name'] == attr_name))['id']
        dataset = self.attr_data[res_attr_id]
        if dataset['metadata'].get('input_method') == 'function':
            value = literal_eval(str(dataset['metadata']['data']))
        else:
            value = dataset['value']
            datatype = dataset['type']
            if datatype == 'timeseries':
                value = pd.read_json(value)
            elif datatype == 'scalar':
                value = float(value)
        return value

    def step(self):
        # CORE MODEL ROUTINE
        self.i += 1
        i = self.i
        date = self.dates[i]

        if i == 0:
            initial_storage = self.initial_storage
        else:
            initial_storage = self.results['Reservoir/Storage'][date - dt.timedelta(days=1)]

        # inflow, demand, ifr = self.data.loc[date]
        inflow = self.data['inflow'][date]
        demand = self.data['demand'][date]
        ifr = self.data['ifr'][date]

        available_water = initial_storage + inflow

        if available_water <= ifr:
            river_release = available_water
            ag_release = 0
        elif available_water <= ifr + demand:
            river_release = ifr
            ag_release = available_water - river_release
        elif available_water <= self.reservoir_capacity:
            river_release = ifr
            ag_release = demand
        else:
            ag_release = demand
            river_release = inflow - ag_release

        storage = initial_storage + inflow - (ag_release + river_release)

        self.results.at[date, 'Agricultural Demand/Delivery'] = ag_release
        self.results.at[date, 'Instream Demand/Delivery'] = river_release
        self.results.at[date, 'Reservoir/Storage'] = storage
        self.results.at[date, 'Outflow/Flow'] = river_release

        return

    def save(self):

        resourcescenarios = []

        for col in self.results:
            type_name, attr_name = col.split('/')
            res = self.resources[type_name]
            ttype = next((tt for tt in self.template['templatetypes'] if tt['name'] == type_name))
            tattr = next((ta for ta in ttype['typeattrs'] if ta['attr']['name'] == attr_name))
            res_attr = next((ra for ra in res['attributes'] if ra['name'] == attr_name), None)
            if not res_attr:
                res_attr = self.conn.add_resource_attribute(res_type='NODE', res_id=res['id'], attr_id=tattr['attr_id'],
                                                            is_var=True)

            df = self.results[[col]]
            df.columns = [0]
            value = df.to_json(date_format='iso')
            resourcescenario = dict(
                resource_attr_id=res_attr['id'],
                dataset={
                    'name': f"{attr_name} for {res['name']}",
                    'type': 'timeseries',
                    'unit_id': tattr['unit_id'],
                    'metadata': {'Source': 'David Rheinheimer', 'Method': 'Python SLOP model.'},
                    'value': value
                }
            )
            resourcescenarios.append(resourcescenario)

        # create the scenario
        now = dt.datetime.now()
        scenario_name = self.scenario_name  # There could be many naming schemes. Here, we will just use the default.
        # First, check and see if a scenario of the same name already exists, and overwrite it if it does. If results
        # are overwritten like this, previously saved figures in the OpenAgua GUI will be updated. In the future,
        # the GUI could be changed to use the latest results of the same run name to be able to see older runs.
        scenario = self.conn.hydra('get_scenario_by_name', self.network['id'], scenario_name, include_data=False)
        if 'error' in scenario:
            logger.info('Creating new scenario.')
            scenario = dict(
                name=scenario_name,
                description='A sloppy model, created in an afternoon',
                layout={'class': 'results', 'run': self.run_name},
                network_id=self.network['id'],
            )
            scenario = self.conn.add_scenario(network_id=self.network['id'], scenario=scenario)['scenario']
            logger.debug(scenario)
        else:
            logger.info('Using old scenario.')

        logger.info('Updating scenario data.')
        for resourcescenario in resourcescenarios:
            scenario.update(
                name=scenario_name,
                layout={'class': 'results', 'run': self.run_name},
                resourcescenarios=[resourcescenario]
            )
        # Note that this is very inefficient; it should be parallelized
        for rs in resourcescenarios:
            if 'id' not in scenario:
                logger.debug(scenario)
            resp = self.conn.hydra('update_resourcedata', scenario['id'], [rs])

        return
