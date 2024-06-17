import logging
import time
import serial
import json
import serial.tools.list_ports

from flatland import Form, Boolean
from microdrop.plugin_helpers import (StepOptionsController, AppDataController,
                                      hub_execute)
from microdrop.plugin_manager import (IPlugin, Plugin, implements, emit_signal,
                                      get_service_instance_by_name,
                                      PluginGlobals, ScheduleRequest)
from pygtkhelpers.gthreads import gtk_threadsafe
from pygtkhelpers.ui.extra_dialogs import yesno, FormViewDialog
from pygtkhelpers.ui.objectlist import PropertyMapper
from pygtkhelpers.utils import dict_to_form
import gtk

from microdrop.app_context import get_app
import numpy as np
import path_helpers as ph
import trollius as asyncio
import pandas as pd
from dropbot.chip import chip_info
from collections import OrderedDict

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

logger = logging.getLogger(__name__)

# Add plugin to `"microdrop.managed"` plugin namespace.
PluginGlobals.push_env('microdrop.managed')


class ValveAutomationPlugin(AppDataController, StepOptionsController, Plugin):
    '''
    This class is automatically registered with the PluginManager.
    '''
    implements(IPlugin)

    plugin_name = str(ph.path(__file__).realpath().parent.name)
    try:
        version = __version__
    except NameError:
        version = 'v0.0.0+unknown'

    AppFields = None

    StepFields = Form.of(Boolean.named('Automate Valves')
                         .using(default=False, optional=True))

    def __init__(self):
        super(ValveAutomationPlugin, self).__init__()
        # The `name` attribute is required in addition to the `plugin_name`
        # attribute because MicroDrop uses it for plugin labels in, for
        # example, the plugin manager dialog.
        self.name = self.plugin_name

        # `dropbot.SerialProxy` instance
        self.dropbot_remote = None

        # Latch to, e.g., config menus, only once
        self.initialized = False

        self.active_step_kwargs = None

        #True if capacitance calibration for has been done
        self.calibrated = False

        #Threshold for when to close Valve
        self.threshold = None

        #Index and value swapped electrode_assignment.csv data
        self.electrode_to_valve = None

    def get_schedule_requests(self, function_name):
        """
        .. versionchanged:: 2.5
            Enable _after_ command plugin and zmq hub to ensure command can be
            registered.

        .. versionchanged:: 2.5.3
            Remove scheduling requests for deprecated `on_step_run()` method.
        """
        if function_name == 'on_plugin_enable':
            return [ScheduleRequest('zika_box_plugin', self.name),
                    ScheduleRequest('dropbot_plugin', self.name),
                    ScheduleRequest('mr_box_plugin', self.name)]
        return []

    def apply_step_options(self, step_options):
        '''
        Apply the specified step options.


        Parameters
        ----------
        step_options : dict
            Dictionary containing the valve plugin options
            for a protocol step.
        '''
        # app = get_app()
        # app_values = self.get_app_values()

        if step_options.get("Automate Valves"):
            # Step option to determine when the electrodes have emptied enough to close the valve to prevent overpelleting
            #Filler that will need to be replaced by a proper way of setting a threshold
            self.threshold = 5e-12

            # Cycle through electrodes until all are below threshold, closing all of the valves

            timeout = 10
            #Start the log dictionary for storing capacitances
            step_log = {}
            #Get actuated electrodes from electrode controller plugin
            electrode_list = self.get_activated_electrodes()
            #Remove any electrodes from the list that aren't assigned to a valve
            electrode_list = [electrode for electrode in electrode_list if electrode in self.electrode_to_valve.index]
            #Add the electrodes that are being used to the log and start timer for logging results
            step_log['Electrode'] = electrode_list
            self.start_time = time.time()

            #Get the valves that are being used this step and sends them to the esp32 so it knows which valves to use
            open_valve_list = [self.electrode_to_valve[electrode]for electrode in electrode_list]
            open_valve_string = ' '.join(str(x) for x in open_valve_list)+" \n"
            self.board.write(open_valve_string)
            logger.info(open_valve_list)
            # Loop through all electrodes until all are below threshold and pumps are turned off
            while electrode_list and time.time()<self.start_time+timeout:
                #measure capacitances of all electrodes with valves still open in series
                capacitances = self.dropbot_remote.channel_capacitances(electrode_list)
                logger.info(capacitances)
                # check all electrodes capacitances vs threshold and add it to a list to send close command to
                electrode_empty_list = [electrode for electrode in electrode_list if capacitances[electrode] <= self.threshold]
                close_valve_list = [self.electrode_to_valve[electrode]for electrode in electrode_empty_list]

                #convert the list of valves to close to a string and append a newline character to it so that the esp32 knows to stop reading
                close_valve_string = ' '.join(str(x) for x in close_valve_list)+" \n"

                #send string of electrodes to close to the esp32
                self.board.write(close_valve_string)

                logger.info("--------------------------------")
                logger.info(close_valve_string)
                logger.info("--------------------------------")

                #Add capacitances to step log
                step_log[time.time()-self.start_time] = capacitances

                #Remove closed valve electrodes from list
                electrode_list = [electrode for electrode in electrode_list if electrode not in electrode_empty_list]

            app=get_app()
            log_dir = app.experiment_log.get_log_path()
            output_path = log_dir.joinpath('valve_log.csv')
            df = pd.DataFrame(step_log)
            df.to_csv(output_path, index=False, header=True)

    @gtk_threadsafe
    def on_plugin_enable(self):
        '''
        Handler called when plugin is enabled.

        For example, when the MicroDrop application is **launched**, or when
        the plugin is **enabled** from the plugin manager dialog.
        '''
        #get list of ports
        ports = serial.tools.list_ports.comports()
        self.board = None
        #try to connect to each port individually, will need a way to test if it's the correct port at some point
        for port, desc, hwid in sorted(ports):
            try:
                self.board = serial.Serial("COM7", 115200, timeout=1)
                logger.info("Connected to valve controller on port: %s", port)
                break
            except Exception as e:
                logger.info("Failed to connect to valve controller on port: %s", port)

        #Warn user if no connection is made
        if not self.board:
            logger.warning("Unable to connect to valve controller")

        #Get electrode assignment and swap them so electrodes are indexes
        plugin_path = ph.path(__file__).parent.realpath()
        assignment_path = (ph.path(plugin_path).joinpath('electrode_assignment.csv'))
        assignment = pd.Series.from_csv(assignment_path)
        self.electrode_to_valve = pd.Series(dict((v,k) for k,v in assignment.iteritems()))

        app = get_app()
        self.tools_menu_item = gtk.MenuItem("Assign electrodes to valves")
        app.main_window_controller.menu_tools.append(self.tools_menu_item)
        self.tools_menu = gtk.Menu()
        self.tools_menu_item.connect("activate",
                                           self.on_edit_electrode_assignment)
        self.tools_menu_item.show()

        try:
            super(ValveAutomationPlugin, self).on_plugin_enable()
        except AttributeError:
            pass

        """
        # Register electrode commands.
        hub_execute('microdrop.command_plugin', 'register_command',
                    command_name='assign_valve', namespace='global',
                    plugin_name=self.name, title='Assign_valve')
        hub_execute('microdrop.command_plugin', 'register_command',
                    command_name='assign_valve_2', namespace='electrode',
                    plugin_name=self.name, title='_Assign_valve_2'
                    'electrode')
        """

    @gtk_threadsafe
    def on_edit_electrode_assignment(self, widget=None, data=None):
        '''
        Display a dialog to manually edit the electrode assignments
        '''
        plugin_path = ph.path(__file__).parent.realpath()
        assignment_path = (ph.path(plugin_path).joinpath('electrode_assignment.csv'))
        assignment = pd.Series.from_csv(assignment_path)
        form = dict_to_form(assignment)
        dialog = FormViewDialog(form, 'Valve: Electrode')
        valid, response = dialog.run()
        if valid:
            new_assignment = pd.Series(response)
            new_assignment.to_csv(assignment_path)
            logger.info("Electrode assignment updated: {}".format(new_assignment))
            self.electrode_to_valve = pd.Series(dict((v,k) for k,v in new_assignment.iteritems()))

    def on_plugin_disable(self):
        '''
        Handler called when plugin is disabled.

        For example, when the MicroDrop application is **closed**, or when the
        plugin is **disabled** from the plugin manager dialog.
        '''

        try:
            self.board.close()
        except:
            pass

        try:
            super(ValveAutomationPlugin, self).on_plugin_disable()
        except AttributeError:
            pass

    def initialize_connection_with_dropbot(self):
        '''
        If the dropbot plugin is installed and enabled, try getting its
        reference.
        '''
        try:
            service = get_service_instance_by_name('dropbot_plugin')
            if service.enabled():
                self.dropbot_remote = service.control_board
            assert (self.dropbot_remote.properties.package_name == 'dropbot')
        except Exception:
            logger.debug('[%s] Could not communicate with Dropbot.', __name__,
                         exc_info=True)
            logger.warning('Could not communicate with DropBot.')


    @asyncio.coroutine
    def on_step_run(self, plugin_kwargs, signals):
        '''
        Handler called whenever a step is executed.

        Plugins that handle this signal **MUST** emit the ``on_step_complete``
        signal once they have completed the step.  The protocol controller will
        wait until all plugins have completed the current step before
        proceeding.
        '''
        # Get latest step field values for this plugin.
        self.active_step_kwargs = plugin_kwargs
        options = plugin_kwargs[self.name]

        if self.dropbot_remote is None:
            self.initialize_connection_with_dropbot()



        # Apply step options
        self.apply_step_options(options)

        self.active_step_kwargs = None
        raise asyncio.Return()

    def on_protocol_run(self):
        '''
        Handler called when a protocol starts running.
        '''
        self.initialize_connection_with_dropbot()


    def get_activated_electrodes(self):
        # Get activated electrodes in form of electrode###
        #electrode_controller_states = self.active_step_kwargs[u'microdrop.electrode_controller_plugin']

        #electrode_list = electrode_controller_states.get('electrode_states', pd.Series())

        states = self.active_step_kwargs[u'microdrop.electrode_controller_plugin'].get(u'electrode_states', None)
        if states is None:
            return
        # logger.info("-"*72)
        # logger.info("All stuff: {}".format(states))
        # logger.info("-"*72)
        app = get_app()
        channels = app.dmf_device.df_electrode_channels.set_index('electrode_id')['channel'].drop_duplicates()
        channels = channels.loc[states.index].tolist()
        # logger.info("Should be electrodes: {}".format(channels))
        # logger.info("-"*72)
        return channels

    def start_monitor(self):
        '''
        .. versionadded:: 2.38

        Start DropBot connection monitor task.
        '''
        if self.monitor_task is not None:
            self.monitor_task.cancel()
            self.control_board = None
            self.dropbot_connected.clear()

        @asyncio.coroutine
        def dropbot_monitor(*args):
            try:
                yield asyncio.From(db.monitor.monitor(*args))
            except asyncio.CancelledError:
                _L().info('Stopped DropBot monitor.')

        self.monitor_task = cancellable(dropbot_monitor)
        thread = threading.Thread(target=self.monitor_task,
                                  args=(self.dropbot_signals, ))
        thread.daemon = True
        thread.start()

    def stop_monitor(self):
        '''
        .. versionadded:: 2.38

        Stop DropBot connection monitor task.
        '''
        if self.dropbot_connected.is_set():
            self.dropbot_connected.clear()
            if self.control_board is not None:
                self.control_board.hv_output_enabled = False
            self.control_board = None
            self.monitor_task.cancel()
            self.monitor_task = None
            self.dropbot_status.on_disconnected()


PluginGlobals.pop_env()
