"""
Authors: Mateusz Lacki & Thilo Schild

Main assumptions:

user might select:
    .../1.8.1/diann K:\rawdata\G2023...8602.d K:\rawdata\G2023...8603.d K:\rawdata\G2023...8604.d 

those need to be copied to the local folder and dealt with diann/whatever else

Results will be kept either in 'user' or 'auto' folder.
'user' folder will contain user defined runs
'auto' folder will contain either developer or acquisition pc runs.

'auto' folder will contain runs indexed by the individual name of the folder
'user' folder will contain runs indexed by the task-id in the queue (which we might want to set to something???)

User runs should (as of now) not result in quant files used by other users.
Only auto runs (i.e. scheduled by the acquisition PC script or by a developers' script) can results in a valid quant file.
Quant file will simply be kept in the folder with results for a given folder.

DIA-NN will be installed to the /usr/diann/[version number]/ folder.
The Linux system must have glibc 2.27 or later (for example, Ubuntu 18.04 or CentOS 8 and later versions are fine).
"""

import tomllib
import functools
import pathlib
import typing
import traceback


def show_traceback(e: Exception) -> None:
    print()
    traceback.print_exc()
    print()


@functools.lru_cache
def parse_config(path_to_config: str) -> dict[str, typing.Any]:
    """Can add later more formats."""
    with open(path_to_config, "rb") as f:
        data = tomllib.load(f)
    return data


config = parse_config("pipeline_config.toml")



# this is done to avoid calling snakemake with the final location from the user side:
# the snakemake call should look like so:
# snakemake -call server/diann/G2023
rule soft_link_server_location:
    output:
        user = directory("user"),
        auto = directory("auto"),
    run:
        try:
            for name, folder in output.items():
                assert pathlib.Path(config[name]).exists(), f"Folder {config[name]} missing."
                shell(f"ln -s {config[name]} {folder}")
        except AssertionError as e:
            show_traceback(e)
            raise e



# install diann version specified in the config file
rule install_diann:
    output:
        # outputs a softlink to the diann installation
        "dianns/{diann_version}/diann"
    run:
        # download diann from github and install it with apt, then remove the .deb installer
        shell(f"wget https://github.com/vdemichev/DiaNN/releases/download/{config[diann_version]}/diann_{config[diann_version]}.deb"),
        shell(f"apt install ./diann_{config[diann_version]}.deb"),
        shell(f"rm ./diann_{config[diann_version]}.deb")
        shell(f"ln -s /usr/diann/{config[diann_version]}/diann-{config[diann_version]} dianns/{config[diann_version]}/diann")



rule get_d_folder_locally_for_worker:
    input:
        "folder_locations.toml"        
    output:
        temp(directory("data/{folder_or_file}.d")),
    params:
        extension = "d"
    run:
        path_map = parse_config(input)
        
        # wildcards.folder_or_file = G210121_003_Slot2-40_1_760.d
        possible_folders = path_map[ wildcards.folder_or_file[0] ]

        # search the possible folders for the "{folder_or_file}.d"
        # path_on_the_server = ...
        shell("cp -r {path_on_the_server} {output.data}")



use rule _locally_for_worker as get_raw_file_locally_for_worker with:
    params:
        extension = "raw"
    output:
        temp("data/{folder_or_file}.raw"),



rule get_quant_file_from_server_or_mock_one:
    """
    Idea behind mocking: make a file with 0 size.
    These should be filtered out later on.
    """
    input:
        "auto"
    output:
        quant = temp("data/{folder}.quant"),
    run:
        quants_location = pathlib.Path(f"{input}/diann/{wildcards.folder}/{wildcards.folder}.quant")

        if quants_location.exists():
            shell("cp -r {quants_location} {output.quant}")
        else:
            shell('touch {output.quant}')



rule run_acquisition_pc_scheduled_diann:
    input:
        "software/{diann_version}/diann",
        "data/{folder_or_file}.d",
    output:
        temp(directory("diann/{diann_version}/auto/{folder_or_file}.d")),
        temp("diann/{diann_version}/auto/{folder_or_file}.d/{folder_or_file}.d.quant"),
    wildcard_constraints:
        extension="d|raw"
    run:
        import subprocess
        # run diann: the script should be prepared so as to use one run



rule copy_run_acquisition_pc_scheduled_diann_to_server:
    input:
        "auto",
        folder="diann/{diann_version}/auto/{folder_or_file}.d",
    output:
        directory("auto/diann/{diann_version}/{folder_or_file_with_extensions}")
    shell:
        "cp -r {input.folder} {output}"



def file_is_empty(path: str) -> bool:
    return os.path.getsize(quant_file) == 0


@functools.lru_cache
def parse_diann_user_config(path):
    result = parse_config(path)
    result["inputs"] = [ 
        *result["raw_folders_or_files"],
        *result["fasta_files"],
    ]
    if "--use-quant" in result["args"]:
        quant_files = []
        for folder_or_file in result["raw_folders_or_files"]:
            quant_file = f"data/{folder_or_file}.quant"
            if not file_is_empty(quant_file):
                quant_files.append(quant_file)
        result["inputs"] = [ *result["inputs"], *quant_files ]
    return result



rule run_user_scheduled_diann:
    """ 
    Run user scheduled diann run:
        * it might comprise multiple datasets (most likely previously analyzed once by the acquisition pc-triggered pipeline)

    'parse_diann_user_config' provides quant files if they were wanted.
    """
    input:
        "dianns/{diann_version}/diann", # thilo, your rule to create diann must provide this as output (a soft link will do)
        lambda wildcards: parse_diann_user_config( f"{wildcards.task_id}.toml" )["inputs"],
    output:
        temp( directory("diann/{diann_version}/user/{task_id}") )
    run:
        import subprocess

        # diann command need to use these:
        # raw_folders = "-f".join(f"data/{folder}" for folder in parse_diann_user_config(f"{wildcards.task_id}.toml")["raw_folders_or_files"])
        # HERE THILO preps a proper cmd
        # parse_diann_user_config(f"{wildcards.task_id}.toml")["args"]# for running subprocess


        subprocess.run(
            cmd,
            shell=True
        )
        # run diann:
        # * remember to set the threads according to config["diann_threads"]

        # PROBLEM TO SOLVE: how to avoid the situation that one pipeline starts copying to output something that another one does?
        # For example: one user wants to analyse G01.d G02.d G03.d, another G03.d G.04.d Both could produce a quant file for G03.d if it is not there.

        # Solution: likely some tasks need to block other tasks and make a prerequisite
        # Solution: likely users should not be able to create quant files, only the pipeline scheduled by us from python or from the acquisition PC.



rule copy_user_scheduled_diann_outputs_to_the_server:
    input:
        "user",
        "diann/{diann_version}/user/{task_id}",
    output:
        directory("user/diann/{diann_version}/{task_id}")
    shell:
        "cp -r {input[1]} {output}"



 
