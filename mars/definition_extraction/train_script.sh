#!/bin/bash
echo 'Running training script, to edit it please go to base_config'
python -m spacy init fill-config base_config.cfg config.cfg
python -m spacy train config.cfg --output ./output
