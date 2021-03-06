"""
This module defines the test argument information list for partadm.py and will 
dynamically be imported by testutils.py to generate the tests for partadm.py.

Refer to the TESTUTILS_README.txt for more information about the usage of this module and testutils.py

test_argslist - is a list of dictionaries, each dictionary has all the necessary info for a test.

"""

test_argslist = [
    { "tc_name" : "version_option", "args" : """--version""", "new_only" : True, },
    { "tc_name" : "help_option_1", "args" : """-h""",  "new_only" : True, },
    { "tc_name" : "help_option_2", "args" : """--help""",  "new_only" : True, },
    { "tc_name" : "no_arg_1", "args" : "", "new_only" : True,},
    { "tc_name" : "no_arg_2", "args" : """-a""", "new_only" : True,},
    { "tc_name" : "debug", "args" : """--debug""", "new_only" : True,},
    { "tc_name" : "combo_options_1", "args" : """-a -d ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_2", "args" : """-a --enable ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_3", "args" : """-d --enable ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_4", "args" : """--enable --disable ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_5", "args" : """--deactivate --activate ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_6", "args" : """-a --deactivate ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_7", "args" : """--fail --unfail ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_8", "args" : """--savestate /tmp/savestate -a""", "new_only" : True,},
    { "tc_name" : "combo_options_9", "args" : """-l --xml""", "new_only" : True,},
    { "tc_name" : "combo_options_10", "args" : """-l --xml""", "new_only" : True,},
    { "tc_name" : "combo_options_11", "args" : """-a --queue q1 ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_12", "args" : """--dump --queue q1 ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_13", "args" : """--savestate /tmp/s --xml""", "new_only" : True,},
    { "tc_name" : "combo_options_14", "args" : """-a -c -b ANL-R00-R01-2048""", "new_only" : True,},
    { "tc_name" : "combo_options_15", "args" : """--list_io -a""", "new_only" : True, 'skip_list' : ['not_bsim'], },
    { "tc_name" : "combo_options_16", "args" : """--list_io -a ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""","new_only" : True, 'skip_list' : ['not_bsim'], },
    { "tc_name" : "combo_options_17", "args" : """--list_io --rmq ANL-R00-M0-512""", "new_only" : True, },
    { "tc_name" : "combo_options_18", "args" : """--list_io --appq ANL-R00-M0-512""", "new_only" : True, },
    { "tc_name" : "combo_options_19", "args" : """--queue q1:q2 --rmq --appq ANL-R00-M0-512""", "new_only" : True, },
    { "tc_name" : "add_option_1", "args" : """-a -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "add_option_2", "args" : """-a --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "add_option_3", "args" : """-a ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "add_option_4", "args" : """-a -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "add_option_5", "args" : """-a -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "delete_option_1", "args" : """-d -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "delete_option_2", "args" : """-d --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "delete_option_3", "args" : """-d ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "delete_option_4", "args" : """-d -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "delete_option_5", "args" : """-d -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "enable_option_1", "args" : """--enable -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "enable_option_2", "args" : """--enable --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "enable_option_3", "args" : """--enable ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "enable_option_4", "args" : """--enable -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "enable_option_5", "args" : """--enable -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "disable_option_1", "args" : """--disable -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "disable_option_2", "args" : """--disable --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "disable_option_3", "args" : """--disable ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "disable_option_4", "args" : """--disable -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "disable_option_5", "args" : """--disable -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "activate_option_1", "args" : """--activate -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "activate_option_2", "args" : """--activate --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "activate_option_3", "args" : """--activate ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "activate_option_4", "args" : """--activate -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "activate_option_5", "args" : """--activate -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "deactivate_option_1", "args" : """--deactivate -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "deactivate_option_2", "args" : """--deactivate --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "deactivate_option_3", "args" : """--deactivate ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "deactivate_option_4", "args" : """--deactivate -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "deactivate_option_5", "args" : """--deactivate -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "fail_option_1", "args" : """--fail -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "fail_option_2", "args" : """--fail --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "fail_option_3", "args" : """--fail ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "fail_option_4", "args" : """--fail -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "fail_option_5", "args" : """--fail -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "unfail_option_1", "args" : """--unfail -r ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "unfail_option_2", "args" : """--unfail --recursive ANL-R00-R01-2048""", "new_only" : True, },
    { "tc_name" : "unfail_option_3", "args" : """--unfail ANL-R00-R01-2048 ANL-R00-1024 ANL-R01-1024""", },
    { "tc_name" : "unfail_option_4", "args" : """--unfail -b ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "unfail_option_5", "args" : """--unfail -c ANL-R00-R01-2048 ANL-R00-1024""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "savestate_option_1", "args" : """--savestate /bad/save""", 'new_only' : True, },
    { "tc_name" : "savestate_option_2", "args" : """--savestate /tmp/save ANL-R00-M0-512""", },
    { "tc_name" : "savestate_option_3", "args" : """--savestate""", },
    { "tc_name" : "savestate_option_4", "args" : """--savestate /tmp/save -c ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "savestate_option_5", "args" : """--savestate /tmp/save -b ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "xml_option_1", "args" : """--xml""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "xml_option_2", "args" : """--xml ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "xml_option_3", "args" : """--xml --recursive ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], "new_only" : True, },
    { "tc_name" : "xml_option_4", "args" : """--xml --blockinfo""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "xml_option_5", "args" : """--xml --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "xml_option_6", "args" : """--xml --recursive --blockinfo""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "xml_option_7", "args" : """--xml --recursive --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "queue_option_1", "args" : """--queue""", },
    { "tc_name" : "queue_option_2", "args" : """--queue q_4:q_3 ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'new_only' : True, },
    { "tc_name" : "queue_option_3", "args" : """--queue q_1:q_2:q_3:q_4 ANL-R00-M0-512""", 'new_only' : True, },
    { "tc_name" : "queue_option_4", "args" : """--queue q_1 -c ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "queue_option_5", "args" : """--queue q_2 -b ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "queue_option_6", "args" : """--queue q_1 -r -b ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "queue_option_7", "args" : """--queue q_2 -r -c ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], "new_only" : True, },
    { "tc_name" : "queue_option_8", "args" : """--queue q_1 --appq -r -c ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "queue_option_9", "args" : """--queue q_2 --rmq -r -c ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "queue_option_10","args" : """--queue q_1 --rmq ANL-R00-M0-512 ANL-R00-M1-512""", 'new_only' : True },
    { "tc_name" : "queue_option_11","args" : """--queue q_1 --appq ANL-R00-M0-512 ANL-R00-M1-512""", 'new_only' : True },
    { "tc_name" : "dump_option_1", "args" : """--dump""", },
    { "tc_name" : "dump_option_2", "args" : """--dump ANL-R00-M0-512""", },
    { "tc_name" : "dump_option_3", "args" : """--dump --recursive ANL-R00-M0-512""", "new_only" : True, },
    { "tc_name" : "dump_option_4", "args" : """--dump --blockinfo""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "dump_option_5", "args" : """--dump --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "dump_option_6", "args" : """--dump --recursive --blockinfo""", 'skip_list' : ['not_bsim'], 'new_only' : True, },
    { "tc_name" : "dump_option_7", "args" : """--dump --recursive --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_stop_option_1", "args" : """--boot-stop""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_stop_option_2", "args" : """--boot-stop ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_stop_option_3", "args" : """--boot-stop --recursive ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], "new_only" : True, },
    { "tc_name" : "boot_stop_option_4", "args" : """--boot-stop --blockinfo""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_stop_option_5", "args" : """--boot-stop --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_stop_option_6", "args" : """--boot-stop --recursive --blockinfo""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_stop_option_7", "args" : """--boot-stop --recursive --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_start_option_1", "args" : """--boot-start""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_start_option_2", "args" : """--boot-start ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_start_option_3", "args" : """--boot-start --recursive ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], "new_only" : True },
    { "tc_name" : "boot_start_option_4", "args" : """--boot-start --blockinfo""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_start_option_5", "args" : """--boot-start --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_start_option_6", "args" : """--boot-start --recursive --blockinfo""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_start_option_7", "args" : """--boot-start --recursive --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_status_option_1", "args" : """--boot-status""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_status_option_2", "args" : """--boot-status ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_status_option_3", "args" : """--boot-status --recursive ANL-R00-M0-512""", 'skip_list' : ['not_bsim'], "new_only" : True, },
    { "tc_name" : "boot_status_option_4", "args" : """--boot-status --blockinfo""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_status_option_5", "args" : """--boot-status --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_status_option_6", "args" : """--boot-status --recursive --blockinfo""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "boot_status_option_7", "args" : """--boot-status --recursive --clean_block""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "list_io_1", "args" : """--list_io""", "new_only" : True, 'skip_list' : ['not_bsim'], },
    { "tc_name" : "list_io_2", "args" : """--list_io ANL-R00-M0-512 ANL-R00-M1-512""", "new_only" : True,  'skip_list' : ['not_bsim'], },
    { "tc_name" : "list_io_3", "args" : """-i""", "new_only" : True, 'skip_list' : ['not_bsim'], },
    { "tc_name" : "add_io_block_1", "args" : """--add_io_block""", 'new_only' : True },
    { "tc_name" : "add_io_block_2", "args" : """--add_io_block ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "del_io_block_1", "args" : """--del_io_block""", 'new_only' : True },
    { "tc_name" : "del_io_block_2", "args" : """--del_io_block ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "boot_io_block_1", "args" : """--boot_io_block""", 'new_only' : True },
    { "tc_name" : "boot_io_block_2", "args" : """--boot_io_block ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "free_io_block_1", "args" : """--free_io_block""", 'new_only' : True },
    { "tc_name" : "free_io_block_2", "args" : """--free_io_block ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "set_io_autoboot_1", "args" : """--set_io_autoboot""", 'new_only' : True },
    { "tc_name" : "set_io_autoboot_2", "args" : """--set_io_autoboot ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'], 'new_only' : True },
    { "tc_name" : "unset_io_autoboot_1", "args" : """--unset_io_autoboot""",'new_only' : True  },
    { "tc_name" : "unset_io_autoboot_2", "args" : """--unset_io_autoboot ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    { "tc_name" : "io_autoboot_start_1", "args" : """--io_autoboot_start""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    { "tc_name" : "io_autoboot_start_2", "args" : """--io_autoboot_start ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    { "tc_name" : "io_autoboot_stop_1", "args" : """--io_autoboot_stop""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    { "tc_name" : "io_autoboot_stop_2", "args" : """--io_autoboot_stop ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    { "tc_name" : "io_autoboot_status_1", "args" : """--io_autoboot_status""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    { "tc_name" : "io_autoboot_status_2", "args" : """--io_autoboot_status ANL-R00-M0-512 ANL-R00-M1-512 ANL-R01-M0-512""", 'skip_list' : ['not_bsim'],'new_only' : True  },
    ]
