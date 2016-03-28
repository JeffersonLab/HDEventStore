
with open("failedruns.lst") as f:
    for line in f:
        run = int(line.strip())
        print "/volatile/halld/offline_monitoring/RunPeriod-2014-10/ver10/idxa/%06d"%run
        #print "/volatile/halld/offline_monitoring/RunPeriod-2014-10/ver10/idxa/%06d/EventStore"%run
