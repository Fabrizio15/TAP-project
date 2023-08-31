import requests
import json
import os
import time
import socket
import berserk

API_KEY = "lip_RNvDmINb3fbTQsj0vBKR"

session = berserk.TokenSession(API_KEY)
client = berserk.Client(session=session)

hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)

TCP_IP = ip_address
TCP_PORT = 5005
RETRY_DELAY = 20

headers = {'Accept': 'application/x-ndjson'}
params = {"nb" : 30,"tags":False,"opening":True}
params_2 = {"tags":False,"opening":True}

# function to send data to logstash

def json_create(id,speed,status,white,black,type,white_r = 0,black_r = 0,opening="",winner = None):
    data = {
        "id" : id,
        "speed": speed,
        "status": status,
        "white": white,
        "black": black,
        "white_rating": white_r,
        "black_rating": black_r,
        "opening_name":opening,
        "type": type,
        #"moves": moves
    }
    if winner is not None:
        data["winner"] = winner

    print("trying to send data:")
    print(data)

    connected = False
    while not connected:
        try:
            # Create a TCP/IP socket and connect to Logstash
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((TCP_IP, TCP_PORT))
            sock.sendall(json.dumps(data).encode('utf-8'))
            sock.close()
            connected = True
        except ConnectionRefusedError:
            print(f"Connection to {TCP_IP}:{TCP_PORT} refused. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

# list with games id (must not exceed 300)
blitz_ids = []
rapid_ids = []

# waiting for logstash to be on
#time.sleep(60)

while True:
    print("starting in 5 seconds")
    time.sleep(5)

    # fetching blitz games
    blitz_games = requests.get("https://lichess.org/api/tv/blitz",headers=headers,params=params)
    if blitz_games.status_code == 429:
        print("Rate limit exceeded, waiting 1 minute")
        time.sleep(60)
    
    #print(blitz_games.text.split("\n")[:-1])

    print("\nblitz games processed")

    time.sleep(3)

    rapid_games = requests.get("https://lichess.org/api/tv/rapid",headers=headers,params=params)
    if rapid_games.status_code == 429:
        print("Rate limit exceeded, waiting 1 minute")
        time.sleep(60)

    print("\nrapid games processed\n")

    # the data is in x-ndjson, we need to split into single json games
    blitz_games_by_line = blitz_games.text.strip().split("\n")
    rapid_games_by_line = rapid_games.text.strip().split("\n")

    # this for is used to send games not ended yet and updating the list
    for entry in blitz_games_by_line:
        game = json.loads(entry)

        if game["status"] == "started":
            a1 = game["id"]
            a2 = game["speed"]
            a3 = game["status"]
            a4 = game["players"]["white"]["user"]["name"]
            a5 = game["players"]["black"]["user"]["name"]
            #a6 = game["moves"]
            a7 = "real_time"
            a8 = game["players"]["white"]["rating"]
            a9 = game["players"]["black"]["rating"]
            json_create(id=a1,speed=a2,status=a3,white=a4,black=a5,type=a7,white_r=a8,black_r=a9)

        if game["id"] not in blitz_ids:

            #print("aggiungo il seguente id:")
            print(game["id"])
            blitz_ids.append(game["id"])

    for entry in rapid_games_by_line:
        game = json.loads(entry)

        if game["status"] == "started":
            a1 = game["id"]
            a2 = game["speed"]
            a3 = game["status"]
            a4 = game["players"]["white"]["user"]["name"]
            a5 = game["players"]["black"]["user"]["name"]
            #a6 = game["moves"]
            a7 = "real_time"
            a8 = game["players"]["white"]["rating"]
            a9 = game["players"]["black"]["rating"]
            json_create(id=a1,speed=a2,status=a3,white=a4,black=a5,type=a7,white_r=a8,black_r=a9)

        if game["id"] not in blitz_ids:

            #print("aggiungo il seguente id:")
            print(game["id"])
            rapid_ids.append(game["id"])

    print("\nwaiting 30 seconds\n")
    time.sleep(30)

    listed_blitz_games = ",".join(blitz_ids)
    listed_rapid_games = ",".join(rapid_ids)

    # prendi la funzione post e chiedi se status è created o started, se no costruisci il json e mandi

    new_blitz = requests.post("https://lichess.org/api/games/export/_ids", data=listed_blitz_games,headers=headers,params=params_2)
    if new_blitz.status_code == 429:
        print("Rate limit exceeded, waiting 1 minute")
        print("no blits games this time")
        time.sleep(60)
    else:
        print("\nnew blits loaded\n")

    time.sleep(3)

    new_rapid = requests.post("https://lichess.org/api/games/export/_ids", data=listed_rapid_games,headers=headers,params=params_2)
    if new_rapid.status_code == 429:
        print("Rate limit exceeded, waiting 1 minute")
        print("no rapid games this time")
        time.sleep(60)
    else:
        print("\nnew rapids loaded\n")

    # the data is in x-ndjson, we need to split into single json games
    new_blitz_by_line = new_blitz.text.strip().split("\n")
    new_rapid_by_line = new_rapid.text.strip().split("\n")


    for entry in new_blitz_by_line:

        #print("\n",entry)
        game = json.loads(entry)

        if game["status"] != "started":

            print("updating blits list and sending to logstash")
            print(game["id"])

            a1 = game["id"]
            #a2 = game["rated"]
            a3 = game["speed"]
            a4 = game["status"]
            a5 = game["players"]["white"]["user"]["name"]
            a6 = game["players"]["black"]["user"]["name"]
            a8 = game["players"]["white"]["rating"]
            a9 = game["players"]["black"]["rating"]
            a7 = "ended"


            if "\"opening\":{" in entry:
                a10 = game["opening"]["name"]
            else:
                a10 = None
                print("No opening for this game")

            if "\"winner\":" in entry:
                a11 = game["winner"]
            else:
                a11 = None
                print("No winner for this game")

            json_create(id=a1,speed=a3,status=a4,white=a5,black=a6,type=a7,white_r=a8,black_r=a9,opening=a10,winner=a11)

            blitz_ids.remove(game["id"])
    
    for entry in new_rapid_by_line:

        print("\n",entry)

        game = json.loads(entry)
        if game["status"] != "started":

            print("updating rapid list and sending to logstash")
            print(game["id"])

            a1 = game["id"]
            #a2 = game["rated"]
            a3 = game["speed"]
            a4 = game["status"]
            a5 = game["players"]["white"]["user"]["name"]
            a6 = game["players"]["black"]["user"]["name"]
            a8 = game["players"]["white"]["rating"]
            a9 = game["players"]["black"]["rating"]
            a7 = "ended"


            if "\"opening\":{" in entry:
                a10 = game["opening"]["name"]
            else:
                a10 = None
                print("No opening for this game")

            if "\"winner\":" in entry:
                a11 = game["winner"]
            else:
                a11 = None
                print("No winner for this game")

            json_create(id=a1,speed=a3,status=a4,white=a5,black=a6,type=a7,white_r=a8,black_r=a9,opening=a10,winner=a11)

            rapid_ids.remove(game["id"])
    
    print("DIM lists:\n",len(blitz_ids),"\n",len(rapid_ids))

    print("completed, waiting 60 seconds for other games")
    time.sleep(60)