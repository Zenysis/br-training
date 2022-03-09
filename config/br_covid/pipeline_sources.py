from models.python.config.pipeline_sources_settings import (
    PipelineSource,
    SharedPipelineStepType,
)

PIPELINE_CONFIG = [
    PipelineSource(source_id='confidence_interval', display_name='CI'),
    PipelineSource(source_id='corrections', display_name='CI Corrections'),
    PipelineSource(source_id='covid', display_name='COVID'),
    PipelineSource(
        source_id='gis',
        display_name='GIS',
        excluded_shared_steps=[
            SharedPipelineStepType.FILL_DIMENSION_DATA,
            SharedPipelineStepType.SYNC_DIGEST_FILES,
            SharedPipelineStepType.POPULATE_PIPELINE_RUN_METADATA,
        ],
    ),
    PipelineSource(source_id='mortality', display_name='Mortality'),
    PipelineSource(
        source_id='population',
        display_name='Population',
        extra_shared_steps=[SharedPipelineStepType.PATCH_LOCATIONS],
    ),
    PipelineSource(
        source_id='legacy_sim',
        display_name='Legacy SIM',
        extra_shared_steps=[SharedPipelineStepType.PATCH_LOCATIONS],
    ),
    PipelineSource(
        source_id='sim',
        display_name='SIM',
        extra_shared_steps=[SharedPipelineStepType.PATCH_LOCATIONS],
        # Remove SIM from this step and run it alone to manage memory better.
        excluded_shared_steps=[SharedPipelineStepType.FILL_DIMENSION_DATA],
    ),
    PipelineSource(
        source_id='sinan',
        display_name='SINAN',
        extra_shared_steps=[SharedPipelineStepType.PATCH_LOCATIONS],
    ),
    PipelineSource(
        source_id='sivep',
        display_name='SIVEP',
        extra_shared_steps=[SharedPipelineStepType.PATCH_LOCATIONS],
    ),
]
