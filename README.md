# Big Data Analytics: HW#4
A MapReduce program for analyzing the check-in records in social networks

## Goal

A MapReduce program for analyzing the check-in records in social networks.

## Input

Check-in records in social networking site Gowalla

- Time and location information of check-ins made by users
- Friendship network of Gowalla users

### Dataset Information

- Gowalla is a location-based social networking website where users share their locations by checking-in

- The friendship network is undirected and was collected using their public API

  - It consists of 196,591 nodes and 950,327 edges

  - A total of 6,442,890 check-ins of these users during the period of Feb. 2009 - Oct. 2010

- Input format: 2 CSV files

  - Time and location information of check-ins made by users: <user_id, checkin_time, latitude, longitude, location_id>

  - Friendship network of Gowalla users: <user_id, user_id>

    

## Output

1. Lists the top checked-in locations (most popular)

2. Lists the top checked-in users

3. Lists the most popular time for check-ins (where time is divided into slots by hours, for example, 7:00-8:00 or 18:00-19:00)

   All of the above results *should* be sorted in descending order of the number of check-ins
   
4. Lists the locations with the largest “check-in community”, sorted in descending order of the number of people in the community.

   The “check-in community” is defined as the people who check-in the same location are also friends

### Output Format

1. a sorted list of locations with check-in frequencies
<location_id>, <freq>
2. a sorted list of users with check-in frequencies
<user_id>, <freq>
3. a sorted list of hours with check-in frequencies
<hour>, <freq>
4. a sorted list of locations with the largest community size
<location_id>, <largest_community_size> 

