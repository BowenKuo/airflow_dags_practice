#### Main: Retrieve GA data to R environment ================
# we connect to Google API with Google Analytics Reporting API v4
# ref: http://code.markedmondson.me/googleAnalyticsR/articles/v4.html
base::require(googleAnalyticsR)
#### Section: Connect to GA reporting API ----------------
# conneccting GA reporting API is in the way of system account,
# NOTE: please setup your own ga_service_account_json_path, and remember NOT to
#       upload your system account json file to github repository.
ga_service_account_json_path <- "/etc/ga_service_account/bonio-da-958b900cd287.json"    # User-defined
ga_client_secret_json_path <- "/etc/ga_client_secret/client_secret_953933740389-gdq7ift55a027a26vjvv37j5mkfcvue3.apps.googleusercontent.com.json"
googleAuthR::gar_set_client(json = ga_client_secret_json_path)
googleAnalyticsR::ga_auth(json_file = ga_service_account_json_path)

view_id <- 166246324    ## PaGamO::1-2 User ID 資料檢視
# setup selecting date range
date_range <- c(start = "2020-09-09", end = "2020-09-09")    # User-defined

#### Section: GA reporting APIv4 demo ----------------
# check out avaiable DIMENSON and METRIC,
# ref: https://ga-dev-tools.appspot.com/dimensions-metrics-explorer
# base::View(googleAnalyticsR::ga_meta())
# dimensions <- base::c("ga:dimension12", "date")
# metrics <- base::c("ga:sessions", "ga:sessionDuration")
# dat <- googleAnalyticsR::google_analytics(viewId = view_id,
#                                           date_range = date_range,
#                                           dimensions = dimensions,
#                                           metrics = metrics,
#                                           anti_sample = TRUE,    # diable sampling while fetching
#                                           max = -1)              # get all rows of data

#### Section: Get activity data given user IDs ----------------
# get user IDs whose number of session > 0, means being active in date range.
uids <- googleAnalyticsR::google_analytics(
  viewId = view_id,
  date_range = date_range,
  metrics = "sessions",
  dimensions = "ga:dimension12",
  # User-defined diemnsion (which is UserID here)
  met_filters = googleAnalyticsR::filter_clause_ga4(base::list(
    googleAnalyticsR::met_filter(
      metric = "sessions",
      operator = "GREATER_THAN",
      comparisonValue = 0
    )
  )),
  anti_sample = TRUE,
  max = -1
)
uids <- uids$dimension12
# get activity data given the list of user IDs
activity_dat <- googleAnalyticsR::ga_clientid_activity(ids = uids[1:10],
                                                       viewId = view_id,
                                                       id_type = "USER_ID",
                                                       date_range = date_range)
# merge sessions and hits together depends on sessionId and id (user ID)
all_activity_dat <- base::merge(
  x = activity_dat$sessions,
  y = activity_dat$hits,
  by = base::c("sessionId", "id")
)
# drop columns in the format of list, because these columns will cause an error
# while parsing dataframe columns to bq_fields
drop_columns <- base::sapply(all_activity_dat, is.list)
all_activity_dat[, drop_columns] <- NULL    # there are some values in column customDimension,
                                            # I'm not sure whether I should drop this column or not.
# type of column "eventCount" should be integer rather than string
all_activity_dat$eventCount <- base::as.integer(all_activity_dat$eventCount)



#### Main: Import data to BigQuery ================
base::require(bigrquery)

#### Section: Connect to BigQuery API ---------------
bq_service_account_json_path <-ga_service_account_json_path
bigrquery::bq_auth(path = bq_service_account_json_path, )

#### Section: Get selected BQ dataset ---------------
# get avaiable BQ projects
avaiable_bq_projects <- bigrquery::bq_projects()
selected_bq_project_id <- "bonio-da"    # User-defined
# get avaiable BQ dataset in the selected project
avaiable_bq_dataset <- bigrquery::bq_project_datasets(x = selected_bq_project_id)
# search dataset given a pattern
search_dataset_pattern <- "DA_20200908"    # User-defined
selected_bq_dataset_idx <- base::grep(pattern = search_dataset_pattern, x = avaiable_bq_dataset)
if(base::identical(selected_bq_dataset_idx, base::integer(0))) {
  # pattern is not found in the list of datasets
  base::stop(
    base::sprintf(
      "Dataset pattern: \'%s\' is not found in project %s",
      search_dataset_pattern,
      selected_bq_project
    )
  )
}
selected_bq_dataset <- avaiable_bq_dataset[[selected_bq_dataset_idx]]

print("Check GA data dimension: ")
print(dim(all_activity_dat))
stop("In the testing phase right now. Need to stop.")

#### Section: Insert data into table ---------------
# fetch table in the dataset
selected_bq_table_id <- base::sprintf("GA_session_activity_%s", base::paste(base::gsub("-", "_", date_range), collapse = "_to_"))
selected_bq_table <- bigrquery::bq_table(project = selected_bq_dataset$project,
                                         dataset = selected_bq_dataset$dataset,
                                         table = selected_bq_table_id)
if(!bigrquery::bq_table_exists(selected_bq_table)) {
  # table doesn't exist in the dataset, then create the table
  bigrquery::bq_table_create(
    x = selected_bq_table,
    fields = bigrquery::as_bq_fields(all_activity_dat),
    friendly_name = "Session activity of users - Testing",
    description = base::sprintf(
      "The data was extracted from DA between %s, recording every session information based on active user IDs",
      base::paste(date_range, collapse = "_")
    ),
    labels = base::list(
      category = "ga",
      type = "dev",
      start = date_range["start"],
      end = date_range["end"]
    )
  )
}
# start uploading
upload_job <- bigrquery::bq_perform_upload(x = selected_bq_table,
                                           values = all_activity_dat,
                                           fields = bigrquery::as_bq_fields(all_activity_dat),
                                           create_disposition = "CREATE_NEVER",    # table should be created previously
                                           write_disposition = "WRITE_TRUNCATE")   # this table should be origianlly empty.
                                                                                   # if not, it'll be empty by WRITE_TRUNCATE.
# wait for uploading job
googleAnalyticsR::bq_job_wait(upload_job)

#### Section: Create another table which is partition table ---------------
# set partition table name
tbl_name <- selected_bq_table$table
partition_tbl_name <- base::paste0(tbl_name, "_partition")
complete_partition_tbl_name <- base::paste(partition_tbl_name, collapse = ".")    # with project and dataset names
# retrieve name and type of data fields
data_fields <- bigrquery::as_bq_fields(all_activity_dat)
all_filed_names <- base::sapply(data_fields, `[`, "name")
all_filed_types <- base::sapply(data_fields, `[`, "type")
# correct wrong filed types
all_filed_types[all_filed_types == "INTEGER"] <- "INT64"
# reformat data fields to SQL needed
format_data_fields <- base::paste(all_filed_names, all_filed_types,
                            sep = " ", collapse = ", ")
# set partition field
partition_field <- "activityTime"
# generate SQL
sql <- base::sprintf("CREATE TABLE `%s` ( %s ) PARTITION BY DATE(%s)", complete_partition_tbl_name, format_data_fields, partition_field)
# execute SQL
tb <- googleAnalyticsR::bq_project_query(x = selected_bq_dataset$project, query = sql)

#### Section: Insert data into the created partition table ---------------
# fetch table in the dataset
selected_bq_table <- bigrquery::bq_table(project = selected_bq_dataset$project,
                                         dataset = selected_bq_dataset$dataset,
                                         table = partition_tbl_name)
# start uploading
upload_job <- bigrquery::bq_perform_upload(x = selected_bq_table,
                                           values = all_activity_dat,
                                           fields = bigrquery::as_bq_fields(all_activity_dat),
                                           create_disposition = "CREATE_NEVER",    # table should be created previously
                                           write_disposition = "WRITE_TRUNCATE")   # this table should be origianlly empty.

# wait for uploading job
googleAnalyticsR::bq_job_wait(upload_job)
