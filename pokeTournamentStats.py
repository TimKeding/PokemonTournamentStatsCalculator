import csv
import sqlite3
from sqlite3 import Error
import pandas

class PokeStats:
    def __init__(self, pokeTournamentFile):
        self.pokedexLoc = 'pokedex.csv'
        self.teamList = []
        self.pokemonUsage = {}
        self.pokemonTeammateUsage = {}
        self.typeCount = {}
        self.conn = None

        self.setupDatabase()
        self.generateTypeDict()
        self.parsePokeTournamentData(pokeTournamentFile)

    def generateTypeDict(self):
        pokemonTypes = ['Fire', 'Water', 'Grass', 'Electric', 'Normal', 'Fighting', 'Psychic', 'Ghost', 'Dark', 'Rock',
                        'Ground', 'Flying', 'Dragon', 'Steel', 'Fairy', 'Ice', 'Bug', 'Poison']
        for type in pokemonTypes:
            self.typeCount[type] = 0
        return

    def parsePokeTournamentData(self, pokeTournamentData):
        with open(pokeTournamentData) as pokeTournamentFile:
            teams = csv.reader(pokeTournamentFile, delimiter=',')
            for team in teams:
                if team[1] != '':
                    self.teamList.append(team[1:])
        return

    def generateTournamentStats(self):
        for team in self.teamList:
            for firstPokemon in range(len(team)):
                firstPokemonTrimmed = team[firstPokemon].lower().strip().title()
                self.addPokemonTypes(firstPokemonTrimmed)
                self.addPokemonUsage(firstPokemonTrimmed)
                self.addPokemonTeammateUsage(firstPokemonTrimmed, team[firstPokemon+1:])

        return

    def addPokemonTypes(self, pokemonName):
        cur = self.conn.cursor()
        cur.execute("SELECT type_number, type_1, type_2 FROM pokedex where name like '{}'".format(pokemonName))
        row = cur.fetchone()
        if row != None:
            type1 = row[1]
            type2 = row[2]
            self.typeCount[type1] += 1
            if type2 != None:
                self.typeCount[type2] += 1
        else:
            print("Couldn't find: " + pokemonName)
        return

    def addPokemonUsage(self, pokemonName):
        if pokemonName in self.pokemonUsage.keys():
            self.pokemonUsage[pokemonName] += 1
        else:
            self.pokemonUsage[pokemonName] = 1
        return

    def addPokemonTeammateUsage(self, firstPokemon, teammates):
        for secondPokemon in teammates:
            secondPokemonTrimmed = secondPokemon.lower().strip().title()
            teammates = firstPokemon + '+' + secondPokemonTrimmed
            teammatesReversed = secondPokemonTrimmed + '+' + firstPokemon
            if teammates in self.pokemonTeammateUsage.keys():
                self.pokemonTeammateUsage[teammates] += 1
            elif teammatesReversed in self.pokemonTeammateUsage.keys():
                self.pokemonTeammateUsage[teammatesReversed] += 1
            else:
                self.pokemonTeammateUsage[teammates] = 1
        return

    def setupDatabase(self):
        sql_create_pokedex_table = """CREATE TABLE IF NOT EXISTS pokedex (
                                            id integer PRIMARY KEY,
                                            pokedex_number integer NOT NULL,
                                            name text NOT NULL,
                                            german_name text,
                                            japanese_name text,
                                            generation integer NOT NULL,
                                            status text NOT NULL,
                                            species text NOT NULL,
                                            type_number integer NOT NULL,
                                            type_1 text NOT NULL,
                                            type_2 text,
                                            height_m float,
                                            weight_kg float,
                                            abilites_number integer NOT NULL,
                                            ability_1 text NOT NULL,
                                            ability_2 text,
                                            ability_hidden text,
                                            total_points integer NOT NULL,
                                            hp integer NOT NULL,
                                            attack integer NOT NULL,
                                            defense integer NOT NULL,
                                            sp_attack integer NOT NULL,
                                            sp_defense integer NOT NULL,
                                            speed integer NOT NULL,
                                            catch_rate integer,
                                            base_friendship integer,
                                            base_experience integer,
                                            growth_rate text,
                                            egg_type_number integer,
                                            egg_type_1 text,
                                            egg_type_2 text,
                                            percentage_male float,
                                            egg_cycles integer,
                                            against_normal float,
                                            against_fire float,
                                            against_water float,
                                            against_electric float,
                                            against_grass float,
                                            against_ice float,
                                            against_fighting float,
                                            against_poison float,
                                            against_ground float,
                                            against_flying float,
                                            against_psychic float,
                                            against_bug float,
                                            against_rock float,
                                            against_ghost float,
                                            against_dragon float,
                                            against_dark float,
                                            against_steel float,
                                            against_fairy float
                                        );"""

        self.createDBConnection()
        if self.conn is not None:
            self.createTable(self.conn, sql_create_pokedex_table)
            df = pandas.read_csv(self.pokedexLoc, delimiter=',')
            df.to_sql('pokedex', self.conn, if_exists='replace', index=False)
        else:
            print("Error! Cannot create the database connection.")

    def createDBConnection(self):
        dbFile = 'pokemonDatabase.db'
        self.conn = None
        try:
            self.conn = sqlite3.connect(dbFile)
        except Error as e:
            print(e)
        return

    def createTable(self, conn, create_table_sql):
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)
        return

    def finished(self):
        self.conn.close()

    def exportDefaultResults(self):
        pokemonUsageResults = {k: v for k, v in
                               sorted(self.pokemonUsage.items(), key=lambda item: item[1], reverse=True)}
        pokemonTeammateResults = {k: v for k, v in
                                  sorted(self.pokemonTeammateUsage.items(), key=lambda item: item[1], reverse=True)}
        pokemonTypeResults = {k: v for k, v in
                                  sorted(self.typeCount.items(), key=lambda item: item[1], reverse=True)}
        pokemonSingleResults = dict(filter(lambda pokemon: pokemon[1] == 1, pokemonUsageResults.items()))
        pokemonTeammateResults = dict(filter(lambda teammates: teammates[1] > 1, pokemonTeammateResults.items()))

        with open('pokemonTournamentResults.csv', mode='w', newline='') as tournamentResultsFile:
            writer = csv.writer(tournamentResultsFile, delimiter=',')
            writer.writerow(["Total Players", len(self.teamList)])
            writer.writerow(["Total Pokemon", len(self.teamList)*6])
            writer.writerow(["Total Unique Pokemon", len(self.pokemonUsage.keys())])
            writer.writerow(["Number of pokemon only appearing once", len(pokemonSingleResults.keys())])
            writer.writerow(["Most Recurring Types"])
            for type, value in pokemonTypeResults.items():
                writer.writerow([type, value])
            writer.writerow([''])
            writer.writerow(["Pokemon Usage Results"])
            for pokemon, value in pokemonUsageResults.items():
                writer.writerow([pokemon, value, str(value/len(self.teamList)*100) + '%'])
            writer.writerow([''])
            writer.writerow(["Teammate Usage Results"])
            for teammates, value in pokemonTeammateResults.items():
                writer.writerow([teammates, value, str(value/len(self.teamList)*100) + '%'])