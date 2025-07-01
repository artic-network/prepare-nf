#!/usr/bin/env nextflow
nextflow.enable.dsl = 2 

process prepare {
    label 'process_single'
    
    container 'community.wave.seqera.io/library/pip_pandas:692f70e2aa6c3c71'
    conda 'bioconda::python=3.10.12 pandas=2.3.0'

    publishDir "${params.outdir}", mode: "${params.publish_dir_mode}", pattern: 'sample_sheet.csv'

    input:
    val platform
    val amplicon_scheme
    path run_dir
    path metadata

    output:
    path 'sample_sheet.csv', emit: sample_sheet

    script:
    """
    prepare.py \
        --platform ${platform} \
        --amplicon_scheme ${amplicon_scheme} \
        --run_dir ${run_dir} \
        --metadata ${metadata}
    """
}

workflow {

    run_dir = file(params.run_dir, type: "dir", checkIfExists: true)
    metadata = file(params.metadata, type: "file", checkIfExists: true)

    prepare(params.platform, params.amplicon_scheme, run_dir, metadata)
}