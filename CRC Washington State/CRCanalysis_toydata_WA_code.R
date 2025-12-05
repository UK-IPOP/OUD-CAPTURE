#install.packages(tidyverse)
#install.packages(SparseMSE)

require(tidyverse)
require(SparseMSE)
# Update this file location 
crcdata <- read.csv("CRCsimdata_WA.csv",
                    row.names = 1)
# Checking data format
head(crcdata)

#For each strata, within each county, row for each combination of databases and the
# number of individuals observed in each combination
crcdata %>% 
  filter( county == "A" & 
            sex =="F" & 
            age_group == "18_39")
#Quick check of variable types
str(crcdata)

#separate counties into "large" and "small" ones:
min_large_pop <- 500000
crcdata_larger <- crcdata %>%
  filter( pop_est > min_large_pop )

crcdata_smaller <- crcdata %>% 
  filter( pop_est < min_large_pop )

#confidence level we will use for our bootstrap confidence intervals:
CI_alpha <- .05 # (1-CI_alpha)100% CIs


#### Larger, heterogenous counties ####
crc_larger_results <- crcdata_larger %>%
  group_by(county,sex,age_group) %>%
  summarize( OUD_sum = sum(OUD_count) ) %>%
  mutate( OUD_est = NA, OUD_CI_lower = NA, OUD_CI_upper = NA)

for (county_i in unique(crcdata_larger$county)){
  
  crcdata_larger_county_i <- crcdata_larger %>%
    filter( county == county_i )
  
  for (sex_j in c("M","F")){
    for (age_j in unique(crcdata_larger$age_group)){
      crcdata_larger_county_i_strata_j <- crcdata_larger_county_i %>%
        filter( sex == sex_j,
                age_group == age_j )
      
      if (nrow(crcdata_larger_county_i_strata_j)>2){ ## need overlap across databases
        crc_results_j <- estimatepopulation( crcdata_larger_county_i_strata_j %>%  
                                               ungroup() %>%
                                               select(pmp,death,OUD_count) %>%
                                               as.matrix(),
                                             method = "main",   # can switch to "stepwise", 
                                             # with more than 2 linked 
                                             # databases; see also pthresh arg.
                                             alpha = c(CI_alpha/2,1-CI_alpha/2)) 
        ## we'll use the bootstrap method for CIs; could switch to estimatepopulation.0 to get
        ##    estimates only (and speed up computation)
        crc_larger_results[ crc_larger_results$county == county_i &
                              crc_larger_results$sex == sex_j &
                              crc_larger_results$age_group == age_j,
                            c("OUD_est","OUD_CI_lower","OUD_CI_upper")] <- 
          list( OUD_est = crc_results_j$popest,
                OUD_CI_lower = crc_results_j$BCaquantiles[1],
                OUD_CI_upper = crc_results_j$BCaquantiles[2] )
      }}}
}
#Table of Results
crc_larger_results

#Plot of Results 
crc_larger_results %>%
  pivot_longer( c(OUD_sum,OUD_est),
                names_to="Estimate",
                values_to="Count") %>% 
  ggplot(aes( y = age_group,
              col = sex)) + 
  geom_segment(aes(x = OUD_CI_lower,
                   xend = OUD_CI_upper),
               lwd = 2, alpha = .2) + 
  geom_point(aes(x=Count,shape=Estimate),size=3) + 
  facet_wrap(vars(county),labeller="label_both") +
  xlab("Count (Log Scale)") + ylab("Age Group") +
  scale_shape(labels=c("CRC point estimate",
                       "Total observed sum")) +
  theme_bw() +
  theme(legend.position = "bottom") + 
  scale_x_log10()

#### Smaller, homogenous counties ####

crcdata_smaller_collapsed <- crcdata_smaller %>%
  group_by(county,
           pmp,death) %>%
  summarize(OUD_count = sum(OUD_count))
crc_smaller_results <- crcdata_smaller_collapsed %>%
  group_by(county) %>%
  summarize( OUD_sum = sum(OUD_count) ) %>%
  mutate( OUD_est = NA, OUD_CI_lower = NA, OUD_CI_upper = NA)

for (county_i in unique(crcdata_smaller_collapsed$county)){
  
  crcdata_smaller_county_i <- crcdata_smaller_collapsed %>%
    filter( county == county_i )
  
  if (nrow(crcdata_smaller_county_i)>2){ ## need overlap across databases
    crc_results_i <- estimatepopulation( crcdata_smaller_county_i %>%  
                                           ungroup() %>%
                                           select(pmp,death,OUD_count) %>%
                                           as.matrix(),
                                         method = "main",   # can switch to "stepwise", 
                                         # with more than 2 linked 
                                         # databases; see also pthresh arg.
                                         alpha = c(CI_alpha/2,1-CI_alpha/2)) 
    ## we'll use the bootstrap method for CIs; could switch to estimatepopulation.0 to get
    ##    estimates only (and speed up computation)
    crc_smaller_results[ crc_smaller_results$county == county_i,
                         c("OUD_est","OUD_CI_lower","OUD_CI_upper")] <- 
      list( OUD_est = crc_results_i$popest,
            OUD_CI_lower = crc_results_i$BCaquantiles[1],
            OUD_CI_upper = crc_results_i$BCaquantiles[2] )
  }
}
#table of results 
crc_smaller_results

crc_smaller_results %>%
  pivot_longer( c(OUD_sum,OUD_est),
                names_to="Estimate",
                values_to="Count") %>% 
  ggplot(aes( #y = age_group,
    #  col = sex)) + 
    y = county )) +
  geom_segment(aes(x = OUD_CI_lower,
                   xend = OUD_CI_upper),
               lwd = 2, alpha = .2) + 
  geom_point(aes(x=Count,shape=Estimate),size=3) + 
  #facet_wrap(vars(county),labeller="label_both") +
  xlab("Count (Log Scale)") + ylab("County") +
  scale_shape(labels=c("CRC point estimate",
                       "Total observed sum")) +
  theme_bw() +
  theme(legend.position = "bottom") + 
  scale_x_log10()
