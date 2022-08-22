import pandas as pd
from glob import glob
import pyarrow as pa


schema = pyarrow.schema([
    ('snippet',pa.string()),
    ('policy_document_series',pa.string()),
    ('overton_policy_document_series',pa.string()),
    ('pdf_document_id',pa.string()),
    ('policy_source_type',pa.list_(pa.string())),
    ('source_tags',pa.list_(pa.string())),
    ('pdf_url',pa.string()),
    ('language',pa.string()),
    ('title',pa.string()),
    ('translated_title',pa.string()),
    ('pdf_title',pa.string()),
    ('sdgcategories',pa.list_(pa.string())),
    ('policy_source_id',pa.string()),
    ('policy_document_id',pa.string()),
    ('published_on',pa.date32()),
    ('classifications',pa.list_(pa.string())),
    ('entities',pa.list_(pa.string())),
    ('policy_document_url',pa.string()),
    ('policy_source_region',pa.list_(pa.string())),
    ('pdf_thumbnail',pa.string()),
    ('policy_source_country',pa.list_(pa.string())),
    ('policy_document_ids_cited',pa.list_(pa.string())),
    ('authors',pa.list_(pa.string())),
    ('policy_source_title',pa.string()),
    ('policy_source_url',pa.string()),
    ('dois_cited',pa.list_(pa.string())),
    ('overton_document_url',pa.string()),
    ('policy_source_country_iso_codes',pa.list_(pa.string())),
    ('ref_contexts',pa.list_(pa.string())),
])


dfs = []
for name in glob('data/overton/warsaw/*.json'):
    dfs.append(pd.read_json(name, lines=True))


df = pd.concat(dfs)

df=df.reset_index(drop=True)

df['published_on']=pd.to_datetime(df.published_on)
df['entities'] = df['entities'].apply(lambda x: [str(a) for a in x])
df['topics'] = df['entities'].apply(lambda x: [str(a) for a in x])
df.to_parquet('data/overton/AI+ML/dump.parquet')