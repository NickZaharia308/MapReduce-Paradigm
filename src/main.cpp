#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <queue>
#include <vector>
#include <map>
#include <sstream>
#include <cctype>

using namespace std;

struct mapThreadArgs
{
    int id;
    queue<pair<string, int>> *inputFiles;
    pthread_mutex_t *readFromInputFileMutex;
    string currentFileName;
    int currentFileIndex;
};

string filterWord(string& word) {
    string filteredWord;
    for (char c : word) {
        if (isalpha(c)) {
            // This will convert to lowercase and keep only characters from the english alphabet
            filteredWord += tolower(c);
        }
    }
    return filteredWord;
}

void *mapFunc(void *arg)
{
    mapThreadArgs *args = (mapThreadArgs *)arg;

    cout << "Thread " << args->id << " is processing file " << args->currentFileName << " with index " << args->currentFileIndex << endl;
    
    ifstream currentFile(args->currentFileName);

    map<string, int> wordMap;
    string line;

    while (getline(currentFile, line)) {
        istringstream lineStream(line);
        string word;

        while (lineStream >> word) {
            string filteredWord = filterWord(word);
            if (!filteredWord.empty()) {
                wordMap[filteredWord] = args->currentFileIndex;
            }
        }
    }

    currentFile.close();

    // Print the whole map
    for (auto it = wordMap.begin(); it != wordMap.end(); it++) {
        cout << it->first << " " << it->second << endl;
    }

    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    // The format of the program runs will be: ./tema1 <noOfMapThreads> <noOfReduceThreads> <inputFile>
    if (argc != 4)
    {
        cout << "Wrong format!" << endl;
        cout << "The format should be: ./tema1 <noOfMapThreads> <noOfReduceThreads> <inputFile>" << endl;
        return -1;
    }

    // Getting the number of threads for Map and Reduce
    const int noOfMapThreads = stoi(argv[1]);
    const int noOfReduceThreads = stoi(argv[2]);

    // Getting the name of the input file
    const string inputFileName = argv[3];

    pthread_t mapThreads[noOfMapThreads];
    pthread_t reduceThreads[noOfReduceThreads];

    ifstream inputFile(inputFileName);
    if (!inputFile.is_open())
    {
        cout << "File " << inputFileName  << " could not be opened!" << endl;
        return -1;
    }

    // Create the mutex for reading from the input files
    // We don't want more than one thread to try to access a new input file at the same time
    pthread_mutex_t *readFromInputFileMutex = new pthread_mutex_t;
    pthread_mutex_init(readFromInputFileMutex, NULL);

    // The queue of input files, as a pair of the name of the file and the index of the file
    queue<pair<string, int>> *inputFiles = new queue<pair<string, int>>();

    // First line is the number of files
    string firstLine;
    getline(inputFile, firstLine);
    int noOfFiles = stoi(firstLine);

    // Reading the files and adding them in the queue
    for (int i = 1; i <= noOfFiles; i++)
    {
        string nameOfTheCurrentFile;
        inputFile >> nameOfTheCurrentFile;
        inputFiles->push(make_pair(nameOfTheCurrentFile, i));    
    }

    for (int i = 0; i < noOfMapThreads; i++)
    {
        // Check if there are still files to be processed
        if (!inputFiles->empty()) {
            mapThreadArgs *args = new mapThreadArgs();
            args->id = i;
            args->readFromInputFileMutex = readFromInputFileMutex;

            // Get the first file from the queue
            pair<string, int> currentPair = inputFiles->front();
            inputFiles->pop();

            args->currentFileName = currentPair.first;
            args->currentFileIndex = currentPair.second;

            args->inputFiles = inputFiles;

 
            int r = pthread_create(&mapThreads[i], NULL, mapFunc, args);

            if (r) {
                cout << "Error creating thread " << i << endl;
                return -1;
            }
        } else {
            break;
        }   
    }

    void *status;
    for (int i = 0; i < noOfMapThreads; i++) {
		int r = pthread_join(mapThreads[i], &status);

		if (r) {
			cout << "Error joining thread " << i << endl;
            return -1;
		}
	}

    return 0;
}