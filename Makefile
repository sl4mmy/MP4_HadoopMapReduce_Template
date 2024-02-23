all: MP4.zip

MP4.zip: JavaTemplate/TopTitles.java JavaTemplate/TopTitleStatistics.java JavaTemplate/OrphanPages.java JavaTemplate/TopPopularLinks.java JavaTemplate/PopularityLeague.java JavaTemplate/TopReviews.java
	zip -j $(@) $(^)
	cp $(@) ~/Downloads/

clean:
	-rm -f MP4.zip ~/Downloads/MP4.zip

.PHONY: all clean
