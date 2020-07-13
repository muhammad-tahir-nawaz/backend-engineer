package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	TCPServerConnection()
}

// TCPServerConnection connect and Listen on port 4040
func TCPServerConnection() {
	// Listening TCP Connection on port 4040
	ln, err := net.Listen("tcp", ":4040")
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	//Show message in the server
	fmt.Println("Server is running")

	// Geting all record from csv file
	allRecord := loadCsvData()

	for {
		// New Connection starting
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}

		//Show message to the client
		_, err = conn.Write([]byte("Connected to server, send the query in the form of JSON object\n\n"))
		if err != nil {
			panic(err)
		}

		for {
			// We are making a buffer to read data and then reading the data provided by the client
			bs := make([]byte, 1024)
			n, err := conn.Read(bs)

			if err != nil {
				break
			}

			// Geting query date and region separately
			queryRegion, queryDate := processInputData(string(bs[:n]))

			// Performing selection on the basis of query
			resultRecord := selection(allRecord, queryDate, queryRegion)

			// converting data into json and []byte to send it to the client
			resultInByte := processOutputData(resultRecord)

			_, err = conn.Write(resultInByte[:])
			if err != nil {
				break
			}
		}

		conn.Close()
	}
}

// QueryInput is struct used to extract query JSON
type QueryInput struct {
	Query QueryInfo
}

// QueryInfo is struct used to get region and date separately
type QueryInfo struct {
	Region string
	Date   string
}

// processInputData separates the region and date parts from json input and return them
func processInputData(queryInStr string) (string, string) {
	var query QueryInput
	queryByte := []byte(queryInStr)
	json.Unmarshal(queryByte, &query)

	return query.Query.Region, query.Query.Date
}

// ReadCsv reads the csv file and extract all rows from it
func ReadCsv(filename string) ([][]string, error) {
	// Open CSV file
	f, err := os.Open(filename)
	if err != nil {
		return [][]string{}, err
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return [][]string{}, err
	}

	return lines, nil
}

// CsvLine arrange each rows and its columns in csv file
type CsvLine struct {
	Date                    string `json:"date"`
	CumulativeTestPositive  string `json:"positive"`
	CumulativeTestPerformed string `json:"tests"`
	Expired                 string `json:"expired"`
	Admitted                string `json:"admitted"`
	Discharged              string `json:"discharged"`
	Region                  string `json:"region"`
}

// loadCsvData method arrange all data from csv file and load it into slice and return it
func loadCsvData() []CsvLine {
	lines, err := ReadCsv("covid_final_data.csv")
	if err != nil {
		panic(err)
	}
	allRecord := []CsvLine{}

	// Loop through lines & turn into object
	for _, line := range lines {
		data := CsvLine{

			Date:                    line[4] + "20",
			CumulativeTestPositive:  line[2],
			CumulativeTestPerformed: line[3],
			Expired:                 line[6],
			Admitted:                line[10],
			Discharged:              line[5],
			Region:                  line[9],
		}
		allRecord = append(allRecord, data)
	}

	return allRecord
}

// selectionByRegion method is used to perform selection of record with respect to query region
func selectionByRegion(allRecord []CsvLine, queryRegion string) []CsvLine {
	regionalRecord := []CsvLine{}

	for _, row := range allRecord {
		if row.Region == queryRegion {
			regionalRecord = append(regionalRecord, row)
		}
	}

	return regionalRecord
}

// selectionByDate method is used to perform selection of record with respect to query date
func selectionByDate(allRecord []CsvLine, queryDate string) []CsvLine {
	datedRecord := []CsvLine{}

	for _, row := range allRecord {
		if row.Date == queryDate {
			datedRecord = append(datedRecord, row)
		}
	}
	return datedRecord
}

// chageDateFormat method is used to convert the date format to match the query date format with data date format
func changeDateFormat(inputDate string) string {
	s := strings.Split(inputDate, "-")
	outputDate := s[2] + "-" + s[1] + "-" + s[0]
	return outputDate
}

// selection method is used to perform selection from whole data on the basis of given query
func selection(allRecord []CsvLine, queryDate string, queryRegion string) []CsvLine {
	if queryDate != "" && queryRegion != "" {
		queryDate = changeDateFormat(queryDate)
		return (selectionByRegion(selectionByDate(allRecord, queryDate), queryRegion))
	}

	if queryDate != "" && queryRegion == "" {
		queryDate = changeDateFormat(queryDate)
		return (selectionByDate(allRecord, queryDate))
	}

	if queryDate == "" && queryRegion != "" {
		return (selectionByRegion(allRecord, queryRegion))
	}

	return []CsvLine{}
}

// processOutputData method is used to convert array of objects of record and convert it into byte type so it can be sent as response
func processOutputData(resultData []CsvLine) []byte {
	b, err := json.Marshal(resultData)
	if err != nil {
		panic(err)
	}

	dataString := "{\"response\":" + string(b[:]) + "}\n\n"
	return []byte(dataString)
}
