### 1.What is fact

- A user login to app
- A transaction happen
- You run a mile with your fitbit => grain is each step in your mile
=> think it is an action what happen and occur
- Fact are not change => because not SCD so make them easier to model than dimension model in some respect

**Grain is atomic, it is smallest and individual, can't break smaller**

### 2.But why make fact modelings is hard

- Fact data have data 10->100x with dimension size
- Example: dimension user have only 1M user, but have 100M notification fact,
because it happens more frequent
- U need more context than dimension to have effective analysis
  - Example: only a notification -> terrible(only a dimension)
  => Effective: notification sent -> 5 minutes after clicked -> 5 minutes after buy sth
  - Example: Google using 40 shade of blue in a button, from the clicked action of
  user in each color, convert to buy action -> Effective analysis
- Fact duplicate is more common than dimension

***So if you want the fact is effective, must have dimension to join with***

### 3.How does fact modeling work

#### Normalization and denormalization

- Normalized fact don't have dimension attribute in, just foreign key of dimension table
to get information
- Denormalization have some dim attributes for quicker analysis but have more cost of storage
- => Both normalized and denormalized have a place in the world

Differentiate between raw logs and fact data
- Raw logs
  - Ugly schema for OLTP(logs have many in transaction system)can
make worse data analysis
  - Potentially contain duplicates,null,other errors,
  - Have shorter retention
- Fact data
  - Nice column names
  - Quality guaranteed as uniqueness,non-nullable,etc
  - Longer retention
    
