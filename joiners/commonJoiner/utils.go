package commonJoiner

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
)

func ReloadStateFromDisk(joinerType string, joinerID string, moviesHeader []string, dataHeader []string, log *logger.ConsoleLogger, readDataFunc func(string, map[string]*model.Row) error, writeMovieFunc func(*csv.Writer, string, *model.Row) error, writeDataFunc func(*csv.Writer, string, *model.Row) error) (map[string]map[string]*model.Row, map[string]map[string]*model.Row, map[string]uint64) {
	rootDir := fmt.Sprintf("joiner_%s/joiner%s", joinerType, joinerID)
	log.Infof("Reloading pending movies from: %s", rootDir)

	pendingMovies := make(map[string]map[string]*model.Row)
	processedMovies := make(map[string]map[string]*model.Row)
	msgIDs := make(map[string]uint64)
	includeTitle := joinerType == "ratings"
	clientDirs, err := os.ReadDir(rootDir)
	if err != nil {
		return pendingMovies, processedMovies, msgIDs
	}

	for _, clientDir := range clientDirs {

		if !clientDir.IsDir() {
			continue
		}
		clientId := clientDir.Name()
		clientPath := fmt.Sprintf("%s/%s", rootDir, clientId)

		files, err := os.ReadDir(clientPath)
		if err != nil {
			continue
		}

		movies := make(map[string]*model.Row)
		processed := make(map[string]*model.Row)
		input := make(map[string]*model.Row)

		for _, file := range files {
			fileName := fmt.Sprintf("%s/%s", clientPath, file.Name())
			if strings.HasSuffix(file.Name(), ".temp") {
				log.Infof("Removing temporary file: %s", fileName)
				err := os.Remove(fileName)
				if err != nil {
					log.Errorf("Failed to remove temporary file %s: %v", fileName, err)
				}
				continue
			}

			if strings.HasSuffix(file.Name(), ".old") {
				idFileName := fmt.Sprintf("%s/id.txt", clientPath)
				if _, err := os.Stat(idFileName); os.IsNotExist(err) {
					err := os.Rename(fileName, idFileName)
					if err != nil {
						log.Errorf("Failed to rename old file %s to id file %s: %v", fileName, idFileName, err)
					}
				} else {
					log.Infof("Removing old id file: %s", fileName)
					err := os.Remove(fileName)
					if err != nil {
						log.Errorf("Failed to remove old id file %s: %v", fileName, err)
					}
				}
				continue
			}

			if strings.HasPrefix(file.Name(), "processed_movies_") {
				LoadWithRecovery(fileName, func(f string, data map[string]*model.Row) error {
					return ReadMoviesCSVToMap(f, data, includeTitle)
				}, func(f string, data map[string]*model.Row) error {
					return SaveData(clientId, data, moviesHeader, f, log, writeMovieFunc)
				}, processed, log)
			}

			if strings.HasPrefix(file.Name(), "movies_") {
				LoadWithRecovery(fileName, func(f string, data map[string]*model.Row) error {
					return ReadMoviesCSVToMap(f, movies, includeTitle)
				}, func(f string, data map[string]*model.Row) error {
					return SaveData(clientId, data, moviesHeader, f, log, writeMovieFunc)
				}, movies, log)
			}

			if strings.HasPrefix(file.Name(), joinerType) {
				LoadWithRecovery(fileName, func(f string, data map[string]*model.Row) error {
					return readDataFunc(f, data)
				}, func(f string, data map[string]*model.Row) error {
					return SaveData(clientId, data, dataHeader, f, log, writeDataFunc)
				}, input, log)
			}

		}

		idFileName := fmt.Sprintf("%s/id.txt", clientPath)
		getMsgId(idFileName, clientId, msgIDs)

		processedMovies[clientId] = processed

		for movieID, row := range movies {
			if _, ok := processed[movieID]; !ok {
				if _, exists := pendingMovies[clientId]; !exists {
					pendingMovies[clientId] = make(map[string]*model.Row)
				}
				pendingMovies[clientId][movieID] = row
				log.Infof("Loaded pending movie %s", movieID)
			}
		}
	}

	return pendingMovies, processedMovies, msgIDs
}

func getMsgId(filePath string, clientId string, msgIDs map[string]uint64) {
	file, err := os.Open(filePath)
	if err != nil { // If the file doesn't exist, we assume msgID is 0
		msgIDs[clientId] = 0
		return
	}
	defer file.Close()

	var msgID uint64
	_, err = fmt.Fscanf(file, "%d", &msgID)
	if err != nil && err != io.EOF { // If there's an error reading the file, we assume msgID is 0. Should not happen.
		msgIDs[clientId] = 0
		return
	}

	msgIDs[clientId] = msgID
}

func SaveMsgId(filePath string, msgID uint64) error {
	tempFile := filePath + ".temp"
	file, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", tempFile, err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d", msgID)
	if err != nil {
		return fmt.Errorf("failed to write msgID to file %s: %w", tempFile, err)
	}

	file.Close()

	if _, err := os.Stat(filePath); err == nil {
		if err := os.Rename(filePath, filePath+".old"); err != nil {
			return fmt.Errorf("failed to rename old file %s: %w", filePath, err)
		}
	}

	if err := os.Rename(tempFile, filePath); err != nil {
		return fmt.Errorf("failed to rename temp file %s to %s: %w", tempFile, filePath, err)
	}
	if _, err := os.Stat(filePath + ".old"); err == nil {
		if err := os.Remove(filePath + ".old"); err != nil {
			return fmt.Errorf("failed to remove old file %s: %w", filePath+".old", err)
		}
	}

	return nil
}

func LoadWithRecovery(fileName string, loader func(string, map[string]*model.Row) error, saver func(string, map[string]*model.Row) error, data map[string]*model.Row, log *logger.ConsoleLogger) {
	if strings.HasSuffix(fileName, ".corrupt") {
		if err := os.Rename(fileName, fileName[:len(fileName)-8]); err != nil {
			log.Errorf("Failed to rename corrupt file %s: %v", fileName, err)
			return
		}
	}
	if err := loader(fileName, data); err != nil {
		log.Warnf("Corrupt file detected: %s. Attempting recovery.", fileName)
		corruptFile := fileName + ".corrupt"
		if err := os.Rename(fileName, corruptFile); err != nil {
			log.Errorf("Failed to rename corrupt file %s: %v", fileName, err)
			return
		}
		tempFileName := fileName + ".temp"

		if err := saver(tempFileName, data); err != nil {
			log.Errorf("Failed to regenerate file %s: %v", fileName, err)
			return
		}
		log.Infof("Successfully regenerated file %s from %s", tempFileName, corruptFile)
		if err := os.Rename(tempFileName, fileName); err != nil {
			log.Errorf("Failed to rename temp file %s to %s: %v", tempFileName, fileName, err)
			return
		}
		log.Infof("Removed corrupt file %s", corruptFile)
		err = os.Remove(corruptFile)
		if err != nil {
			log.Errorf("Failed to remove corrupt file %s: %v", fileName, err)
		}
	}
}

func ReadMoviesCSVToMap(fileName string, movies map[string]*model.Row, includeTitle bool) error {
	expectedLen := 1
	if includeTitle {
		expectedLen = 2
	}
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from file %s: %w", fileName, err)
	}
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < expectedLen {
			return fmt.Errorf("invalid row in file %s: %v", fileName, err)
		}
		movieID := data[0]
		if movieID == "" {
			return fmt.Errorf("empty movieID in file %s", fileName)
		}
		movies[movieID] = &model.Row{Strings: map[string]string{"movieID": movieID}}
		if includeTitle {
			title := data[1]
			if title == "" {
				return fmt.Errorf("empty title for movieID %s in file %s", movieID, fileName)
			}
			movies[movieID].Strings["title"] = title
		}

	}
	return nil

}

func ReadCreditsCSVToMap(fileName string, credits map[string]*model.Row) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from file %s: %w", fileName, err)
	}
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil || len(data) < 2 {
			return fmt.Errorf("invalid row in file %s: %v", fileName, err)
		}

		movieID := data[0]
		if movieID == "" {
			return fmt.Errorf("empty movieID in file %s", fileName)
		}
		cast := data[1]
		var castList []string
		if err := json.Unmarshal([]byte(cast), &castList); err != nil {
			return fmt.Errorf("failed to unmarshal cast from file %s: %v", fileName, err)
		}
		credits[movieID] = &model.Row{
			Strings: map[string]string{"ID": movieID},
			Arrays:  map[string][]string{"cast": castList},
		}
	}

	return nil
}

func ReadRatingsCSVToMap(fileName string, ratings map[string]*model.Row) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header from file %s: %w", fileName, err)
	}
	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil || len(data) < 2 {
			return fmt.Errorf("invalid row in file %s: %v", fileName, err)
		}

		movieID := data[0]
		if movieID == "" {
			return fmt.Errorf("empty movieID in file %s", fileName)
		}
		ratingString := data[1]
		rating, err := strconv.ParseFloat(ratingString, 64)
		if err != nil {
			return fmt.Errorf("failed to parse rating: %v; rating = %v", err, ratingString)

		}

		ratings[movieID] = &model.Row{
			Strings: map[string]string{"movieID": movieID},
			Floats:  map[string]float64{"avg_rating": rating},
		}
	}

	return nil
}

func SaveData(clientID string, data map[string]*model.Row, header []string, fileName string, log *logger.ConsoleLogger, writeFunction func(*csv.Writer, string, *model.Row) error) error {
	writeHeader := false
	if stat, err := os.Stat(fileName); err == nil {
		if stat.Size() == 0 {
			writeHeader = true
		}
	} else if os.IsNotExist(err) {
		writeHeader = true
	} else {
		log.Errorf("Error checking file status: %v", err)
		return err
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %s", fileName)
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeader {
		if err := writer.Write(header); err != nil {
			log.Errorf("Failed to write CSV header: %v", err)
			return err
		}
	}

	for id, row := range data {
		if err := writeFunction(writer, id, row); err != nil {
			log.Errorf("Failed to write row with ID %s: %v", id, err)
			return err
		}

	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Errorf("Flush error writing CSV: %v", err)
		return err
	}
	return nil
}

func WriteCreditRow(writer *csv.Writer, id string, credit *model.Row) error {
	castString, err := json.Marshal(credit.Arrays["cast"])
	if err != nil {
		return err
	}

	return writer.Write([]string{credit.Strings["ID"], string(castString)})
}

func WriteRatingRow(writer *csv.Writer, id string, rating *model.Row) error {
	ratingString := fmt.Sprintf("%f", rating.Floats["avg_rating"])
	return writer.Write([]string{rating.Strings["movieID"], ratingString})
}

func WriteMovieID(writer *csv.Writer, id string, _ *model.Row) error {
	return writer.Write([]string{id})
}

func WriteMovieRow(writer *csv.Writer, id string, row *model.Row) error {
	return writer.Write([]string{id, row.Strings["title"]})
}
