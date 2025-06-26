#!/usr/bin/env nextflow
nextflow.enable.dsl = 2 

process prepare {
    label 'process_single'
    publishDir "${params.outdir}", mode: "${params.publish_dir_mode}", pattern: 'sample_sheet.csv'

    input:
    val platform
    val amplicon_scheme
    path run_dir
    path metadata

    output:
    path 'samplesheet.csv', emit: sample_sheet

    script:
    """
    python3 prepare.py \
        --platform ${platform} \
        --amplicon-scheme ${amplicon_scheme} \
        --run-dir ${run_dir} \
        --metadata ${metadata}
    """
}

workflow {

    main:
    run_dir = file(params.run_dir, type: "dir", checkIfExists: true)
    metadata = file(params.metadata, type: "file", checkIfExists: true)
k   
    prepare(params.platform, params.amplicon_scheme, run_dir, metadata)

    emit:
    prepare.out.sample_sheet
}