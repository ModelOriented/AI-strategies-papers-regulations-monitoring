#!/bin/bash
echo 'Running training script, to edit it please go to base_config'

python -m spacy init fill-config transformer_config.cfg config_trf.cfg
echo 'Filled config'
python -m spacy train config_trf.cfg  --gpu-id 0 --output ./output3
