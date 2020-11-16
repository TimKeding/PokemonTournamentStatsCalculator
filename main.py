from pokeTournamentStats import PokeStats
import sys

def main():
    if len(sys.argv) == 2:
        tournamentFile = str(sys.argv[1])
        pokeStats = PokeStats(tournamentFile)
        pokeStats.generateTournamentStats()
        pokeStats.exportDefaultResults()
        pokeStats.finished()
    else:
        print("No file given")

if __name__ == '__main__':
    main()