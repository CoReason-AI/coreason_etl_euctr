# Mapping Guide

This guide explains how the raw HTML content from the EU Clinical Trials Register (EU CTR) is mapped to the structured Silver Layer tables in PostgreSQL.

## 1. Overview

The parsing logic extracts data from the specific Lettered Sections (e.g., A, B, D) of the EU CTR protocol pages.

| Source System | Source Format | Target Schema |
| :--- | :--- | :--- |
| **EU CTR** | HTML (Unstructured) | **Silver Layer** (Normalized SQL) |

## 2. Core Trial Mapping (`eu_trials`)

**Table:** `eu_trials`
**Primary Key:** `eudract_number`

| SQL Column | Source HTML Section | HTML Label / Extraction Logic | Notes |
| :--- | :--- | :--- | :--- |
| `eudract_number` | Header | `EudraCT Number` | Acts as the unique identifier. |
| `sponsor_name` | **B. Sponsor** | `Name of Sponsor` | Extracted from Section B.1.1. |
| `trial_title` | **A. Protocol** | `Full title of the trial...` | Fallback to "Title of the trial for lay people" if full title is missing. |
| `start_date` | Header / Admin | `Date of Competent Authority Decision` | Fallback to `Date record first entered`. Parsed to YYYY-MM-DD. |
| `trial_status` | Header | `Trial Status` | Fallback to `Status of the trial`. |
| `url_source` | N/A | *Metadata* | The URL from which the page was scraped (e.g., GB vs DE). |
| `age_groups` | **F. Population** | `F.1.1 Adults`, `F.1.2 Children` | Checkboxes marked "Yes" are collected into an array. |

## 3. Drugs Mapping (`eu_trial_drugs`)

**Table:** `eu_trial_drugs`
**Relationship:** One-to-Many with `eu_trials`

Data is extracted by iterating through **Section D (IMP)**. Since a trial can have multiple drugs (placebo, active, comparator), multiple rows may be generated per trial.

| SQL Column | Source HTML Section | HTML Label / Extraction Logic |
| :--- | :--- | :--- |
| `drug_name` | **D. IMP** | `Trade name` or `Product Name` |
| `active_ingredient` | **D. IMP** | `Name of Active Substance` or `Active Substance` |
| `cas_number` | **D. IMP** | `CAS Number` |
| `pharmaceutical_form`| **D. IMP** | `Pharmaceutical form` |

## 4. Conditions Mapping (`eu_trial_conditions`)

**Table:** `eu_trial_conditions`
**Relationship:** One-to-Many with `eu_trials`

Data is extracted from **Section E (General Information on the Trial)**.

| SQL Column | Source HTML Section | HTML Label / Extraction Logic |
| :--- | :--- | :--- |
| `condition_name` | **E. Condition** | `Medical condition(s) being investigated` |
| `meddra_code` | **E. Condition** | `MedDRA version` / `MedDRA level` | Combined if both exist. |
