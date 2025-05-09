#!/bin/bash

activities=(
    allergenicity
    antibacterial
    anticancer
    antifungal
    antimicrobial
    antimalarial
    antiparasitic
    antioxidative
    antiviral
    bitter
    blood-brain-barrier
    cell-penetrating
    dipeptidyl-peptidaseiv
    neuropeptide
    processed
    quorum-sensing
    toxicity
    toxicology
    tumor_t_cell_antigens
    umami
    antihypertensive
)

# Download each activity file
for activity in "${activities[@]}"; do
    python src/uniprot_search_query.py \
        -q "$activity AND reviewed:true" \
        -o "results/uniprot_${activity}.csv"
done