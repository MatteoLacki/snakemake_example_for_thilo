import tomllib
import functools
import pathlib

def parse_config(path_to_config):
    with open(path_to_config, "rb") as f:
        data = tomllib.load(f)
    return data


config = parse_config("pipeline_config.toml")




rule multiple_inputs_from_config:
    input:
        lambda wildcards: parse_config(f"{wildcards.config}.toml")["inputs"]
    output:
        directory("{config}")
    shell:
        """
        cat {input} > {output}
        """


rule copy_d_folder:
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
        shell("cp {path_on_the_server} {output.data}")


rule copy_quant_file:
    output:
        quant = temp("data/{folder_or_file}.{extension}.quant"),
    wildcard_constraints:
        extension="d|raw"
    run:
        quants_location = pathlib.Path(config["diann_quants_folder"]) / f"{wildcards.folder_or_file}.{wildcards.extension}.quant"

        if quants_location.exists():
            shell("cp {quants_location} {output.quant}")
        else:
            shell('touch {output.quant}')


use rule copy_d_folder as copy_raw_file with:
    params:
        extension = "raw"
    output:
        temp("data/{folder_or_file}.raw"),



@functools.lru_cache
def parse_diann_user_config(path):
    result = parse_config(path)
    result["inputs"] = [ *result["raw_folders_or_files"], *result["fasta_files"],  ]
    if "--use-quant" in result["args"]:
        quant_files = []
        for folder_or_file in result["raw_folders_or_files"]:
            quant_file = f"data/{folder_or_file}.quant"
            if os.path.getsize(quant_file) > 0:
                quant_files.append(quant_file)
        result = [*result, *quant_files]
    return result



rule run_diann:
    input:
        "path_to_diann",
        lambda wildcards: parse_diann_user_config(f"{wildcards.task_id}.toml")["inputs"],
    output:
        temp(directory("diann/{task_id}"))
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
        # run diann 



rule soft_link_final_output:
    output:
        directory("server")
    shell:
        f"ln -s {config['output_folder']} {output}"


rule copy_final_outputs:
    input:
        "diann/{task_id}",
        "server",
    output:
        directrory(f"server/diann/{task_id}")
    shell:
        "cp {input[0]} {output}"

# snakemake -call server/diann/G2023
# diann K:\rawdata\G2023...8602.d K:\rawdata\G2023...8603.d K:\rawdata\G2023...8604.d 
 
