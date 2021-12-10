import datetime as dt
from ast import literal_eval
import pandas as pd
from dateutil.parser import parse


class SloppyModel(object):

    def __init__(self, oa_client, network, template, scenario_ids, debug=False):
        baseline_scenarios = [s for s in network['scenarios'] if s['layout']['class'] == 'baseline']
        baseline_scenario = baseline_scenarios[0]
        start = parse(baseline_scenario['start_time'])
        end = parse(baseline_scenario['end_time'])
        timestep = baseline_scenario['time_step']
        freq = {'day': 'D', 'month': 'M'}.get(timestep)
        dates = pd.date_range(start, end, freq=freq)

        self.network = network
        self.template = template
        self.dates = dates[:5] if debug else dates
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

        def get_attr_value(res, attr_name, attribute_data):
            res_attr_id = next((ra for ra in res['attributes'] if ra['name'] == attr_name))['id']
            dataset = attribute_data[res_attr_id]['dataset']
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

        # pull out the resource scenarios (data), overwriting parent scenarios with child scenarios
        resource_scenarios = {}
        attribute_data = {}
        for scenario_id in scenario_ids:

            _scenario_id = scenario_id
            while _scenario_id and _scenario_id not in resource_scenarios:
                scenario = self.conn.get_scenario(_scenario_id, include_data=True)['scenario']
                resource_scenarios[_scenario_id] = scenario['resourcescenarios']
                _scenario_id = scenario['parent_id']

            reversed_scenario_ids = list(reversed(resource_scenarios.keys()))
            for scenario_id in reversed_scenario_ids:
                for resource_scenario in resource_scenarios[scenario_id]:
                    attribute_data[resource_scenario['resource_attr_id']] = resource_scenario

        # inflow
        node = self.get_node_by_type(nodes, template_id, 'Inflow')
        self.resources['Inflow'] = node
        inflow = get_attr_value(node, 'Runoff', attribute_data)
        self.data = inflow * 0.0864
        self.data.columns = ['inflow']

        # ag demand
        node = self.get_node_by_type(nodes, template_id, 'Agricultural Demand')
        self.resources['Agricultural Demand'] = node
        demand = get_attr_value(node, 'Demand', attribute_data)
        self.data['demand'] = demand * 0.0864

        # instream demand
        node = self.get_node_by_type(nodes, template_id, 'Instream Demand')
        self.resources['Instream Demand'] = node
        demand = get_attr_value(node, 'Instream Flow Requirement', attribute_data)
        self.data['ifr'] = demand * 0.0864

        node = self.get_node_by_type(nodes, template_id, 'Reservoir')
        self.resources['Reservoir'] = node
        self.reservoir_capacity = get_attr_value(node, 'Storage Capacity', attribute_data)
        self.initial_storage = get_attr_value(node, 'Initial Storage', attribute_data)

        self.resources['Outflow'] = self.get_node_by_type(nodes, template_id, 'Outflow')

        return

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
                    'name': f"{attr_name} for ${res['name']}",
                    'type': 'timeseries',
                    'unit_id': tattr['unit_id'],
                    'metadata': {'Source': 'David Rheinheimer', 'Method': 'Python SLOP model.'},
                    'value': value
                }
            )
            resourcescenarios.append(resourcescenario)

        # create the scenario
        now = dt.datetime.now()
        scenario = dict(
            name=f'SLOPPY - {now}',
            description='A sloppy model, created in an afternoon',
            layout={'class': 'results'},
            network_id=self.network['id'],
            resourcescenarios=resourcescenarios
        )
        resp = self.conn.add_scenario(network_id=self.network['id'], scenario=scenario)

        return
