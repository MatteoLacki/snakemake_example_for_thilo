import tomllib


def parse_config(path_to_config):
	print(path_to_config)
	with open(path_to_config, "rb") as f:
		data = tomllib.load(f)
	print(data)
	return data


rule multiple_inputs_from_config:
	input:
		lambda wildcards: parse_config(f"{wildcards.config}.toml")["inputs"]
	output:
		"{config}.bububu"
	shell:
		"""
		cat {input} > {output}
		"""

