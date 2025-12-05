#' @title bupe_oud_definition
#'
#' @description 
#'  This function identifies and flags buprenorphine medications that are 
#'   used for treating opioid use disorder (OUD). It filters out buprenorphine 
#'   products not typically used for OUD (e.g., pain patches like Butrans or 
#'   Belbuca) based on product name, dosage form, and strength.
#'
#' @param  
#'  - data: A data frame containing medication records
#'   - active_ingredient: The column name (as string) representing the active ingredient
#'  - drugnamewithStrength: The column name (as string) for the drug name (with strength)
#'   - masterForm: The column name (as string) for dosage form
#'   - strengthPerUnit: The column name (as string) for strength per unit
#'
#' @output 
#'   - A mutated version of the input dataset with a new column `bupe_oud`:
#'       - 1 = include as OUD-related buprenorphine
#'       - 0 = exclude (not OUD-related)
#'
#' @details 
#'    - Includes buprenorphine drugs if they match known OUD brand names
#'    - Excludes known pain-only products (Belbuca, Butrans), patch forms,
#'    - and specific strength values that are unlikely to be used for OUD
#'    - Defaults to including all other buprenorphine products
#' 

bupe_oud_definition <- function(data,
                                active_ingredient, 
                                drugnamewithStrength,
                                masterForm,
                                strengthPerUnit) {
  
  tryCatch({
    # Convert string column names into symbols for tidy evaluation
    active_ingredient <- sym(active_ingredient)
    drugnamewithStrength <- sym(drugnamewithStrength)
    masterForm <- sym(masterForm)
    strengthPerUnit <- sym(strengthPerUnit)
    
    return(
      data %>%
        dplyr::mutate(
          # Standardize text to uppercase 
          drugnamewithStrength = stringr::str_to_upper(!!drugnamewithStrength),
          masterForm = stringr::str_to_upper(!!masterForm),
          active_ingredient = stringr::str_to_upper(!!active_ingredient),
          
          # Remove trailing and leading blanks
          drugnamewithStrength = stringr::str_squish(!!drugnamewithStrength),
          masterForm = stringr::str_squish(!!masterForm),
          active_ingredient = stringr::str_squish(!!active_ingredient),
          
          # Apply logic to define whether the row should be included as buprenorphine for OUD
          bupe_oud = case_when(
            # Include specific OUD brand names
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bUNAVAIL\\b") ~ 1,
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bSUBLOCADE\\b") ~ 1,
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bSUBOXONE\\b") ~ 1,
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bSUBUTEX\\b") ~ 1,
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bZUBSOLV\\b") ~ 1,
            
            # Exclude known pain medications
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bBELBUCA\\b") ~ 0,
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bBUTRANS\\b") ~ 0,
            
            # Exclude patch dosage forms
            active_ingredient %in% c('BUPRENORPHINE') & !!masterForm %in% c("PATCH", "PATCH, EXTENDED RELEASE") ~ 0,
            active_ingredient %in% c('BUPRENORPHINE') & stringr::str_detect(!!drugnamewithStrength, "\\bPATCH\\b") ~ 0,
            
            # Exclude certain strengths (often pain-only formulations)
            active_ingredient %in% c('BUPRENORPHINE') & !!strengthPerUnit %in% c(5, 7.5, 10, 15, 20, 75, 150, 300, 450, 600, 750, 900) ~ 0,
            
            # All other buprenorphine products assumed to be for OUD
            active_ingredient %in% c('BUPRENORPHINE') ~ 1,
            
            # All other products are excluded
            TRUE ~ 0
          )
        )
    )
    
  }, error = function(e) {
    message("Error in function bupe_oud_definition(): ", e)
    stop(e)
  })
}

library(tidyverse)
library(DBI)

con <- DBI::dbConnect(odbc::odbc(), "PMPodbc", timeout = 10)


##Bupe COmparispon
bupe.definition <-
  DBI::dbGetQuery(
    conn = DBI::dbConnect(odbc::odbc(), "PMPodbc", timeout = 10),
    glue::glue_sql(
      "
      SELECT 
      a.drugname, a.drugnamewithStrength,TRIM(UPPER(a.Drug)) as drug, a.masterForm, a.strengthPerUnit,
      SUM(CASE WHEN z.ndcCode IS NOT NULL THEN 1 ELSE 0 END) as OUD_BUPE
      FROM PMP.dbo.pmpall_PrescriptionDrug as a
                LEFT JOIN PMP_Staging.dbo.codbPullsCodeList as z on TRIM(a.ndc) = TRIM(z.ndcCode)
      WHERE TRIM(UPPER(a.Drug)) = 'BUPRENORPHINE'
      GROUP BY a.drugname, a.drugnamewithStrength,a.drug, a.masterForm, a.strengthPerUnit;
      ",
      .con = DBI::dbConnect(odbc::odbc(), "PMPodbc", timeout = 10)
    )
  ) %>% 
  dplyr::mutate(dshs_oud = ifelse(OUD_BUPE >= 1, 1, 0))

bupe.definition2 <- bupe_oud_definition(
  data = bupe.definition,
  active_ingredient = "drug",
  drugnamewithStrength  = "drugnamewithStrength",
  masterForm  = "masterForm",
  strengthPerUnit  = "strengthPerUnit"
)

janitor::tabyl(bupe.definition2, dshs_oud, bupe_oud)
