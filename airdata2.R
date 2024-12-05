library(httr)
library(rvest)
library(dplyr)
library(DBI)
library(RMariaDB)

# Logfilens placering
log_file <- "C:/temp/webscraping_log.txt"

# Funktion til at skrive til logfilen
log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_entry <- paste0("[", timestamp, "] ", message, "\n")
  cat(log_entry, file = log_file, append = TRUE)
}

# Start loggen
log_message("Script startede.")

# Forbind til MySQL
log_message("Opretter forbindelse til MySQL.")
connection <- tryCatch({
  dbConnect(MariaDB(),
            db = "luftdata",
            host = "localhost",
            port = 3306,
            user = "root",
            password = "Loubani1045!")
}, error = function(e) {
  log_message(paste("FEJL: Kunne ikke oprette forbindelse til MySQL:", e$message))
  stop(e)
})
log_message("Forbindelse til MySQL etableret.")

# Hent luftdata
user_agent <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
baseurl <- "https://envs2.au.dk/Luftdata/Presentation/table/Copenhagen/HCAB"
log_message("Henter token fra basen URL.")

GET_response <- GET(url = baseurl, add_headers(`User-Agent` = user_agent))
GET_content <- httr::content(GET_response, as = "text")
token <- tryCatch({
  read_html(GET_content) %>% 
    html_element("input[name='__RequestVerificationToken']") %>% 
    html_attr("value")
}, error = function(e) {
  log_message(paste("FEJL: Kunne ikke hente token:", e$message))
  stop(e)
})
log_message("Token hentet succesfuldt.")

# URLs for byerne
cities <- list(
  HCAB = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Copenhagen/HCAB",
  ANHO = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Rural/ANHO",
  AARH3 = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Aarhus/AARH3",
  RISOE = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Rural/RISOE"
)

# Funktion til at hente tabeller
fetch_table <- function(url, token, city_name) {
  log_message(paste("Henter data for", city_name))
  tryCatch({
    response <- POST(
      url = url,
      add_headers(`User-Agent` = user_agent),
      body = list(`__RequestVerificationToken` = token), 
      encode = "form"
    )
    content(response, as = "text") %>% 
      read_html() %>% 
      html_element("table") %>% 
      html_table()
  }, error = function(e) {
    log_message(paste("FEJL: Kunne ikke hente data for", city_name, ":", e$message))
    NULL
  })
}

# Hent data for alle byer
tables <- list()
for (city_name in names(cities)) {
  table <- fetch_table(cities[[city_name]], token, city_name)
  if (!is.null(table)) {
    tables[[city_name]] <- table
    log_message(paste("Data for", city_name, "hentet succesfuldt."))
  } else {
    log_message(paste("Data for", city_name, "kunne ikke hentes."))
  }
}

# Funktion til at uploade data og opdatere eksisterende data i databasen
upload_to_database <- function(connection, table_name, data) {
  log_message(paste("Uploader data for", table_name, "til MySQL."))
  tryCatch({
    # Hent eksisterende data baseret på "Målt (starttid)" for at undgå duplikering
    existing_data <- dbGetQuery(connection, paste("SELECT `Målt (starttid)` FROM", table_name))
    
    # Filtrér data for at finde de poster, der ikke allerede findes i databasen
    new_data <- data %>% 
      filter(!`Målt (starttid)` %in% existing_data$`Målt (starttid)`)
    
    # Hvis der er nye data, indsættes de
    if (nrow(new_data) > 0) {
      dbWriteTable(
        conn = connection,
        name = table_name,
        value = new_data,
        append = TRUE,
        row.names = FALSE
      )
      log_message(paste("Data for", table_name, "uploadet succesfuldt."))
    } else {
      log_message(paste("Ingen nye data for", table_name, "at uploade."))
    }
  }, error = function(e) {
    log_message(paste("FEJL: Kunne ikke uploade data for", table_name, ":", e$message))
  })
}

# Upload hver tabel til databasen
for (name in names(tables)) {
  upload_to_database(connection, name, tables[[name]])
}

# Luk forbindelsen
log_message("Lukker forbindelsen til MySQL.")
dbDisconnect(connection)
log_message("Script afsluttet.")
