+++
date = "2017-05-17T15:36:57Z"
title = "Quick Start - POJOs"
[menu.main]
  parent = "MongoDB Reactive Streams"
  identifier = "Reactive Quick Start - POJOs"
  weight = 15
  pre = "<i class='fa'></i>"
+++

# MongoDB Reactive Streams Driver Quick Start - POJOs

{{% note %}}
POJOs stands for Plain Old Java Objects.

The following code snippets come from the [`PojoQuickTour.java`]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/PojoQuickTour.java">}}) example code
that can be found with the driver source on github.
{{% /note %}}

{{% note class="important" %}}
This guide uses the `Subscriber` implementations as covered in the [Quick Start Primer]({{< relref "driver-reactive/getting-started/quick-start-primer.md" >}}).
{{% /note %}}


## Prerequisites

- A running MongoDB on localhost using the default port for MongoDB `27017`

- MongoDB Reactive Streams Driver. See [Installation]({{< relref "driver-reactive/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- Quick Start. This guide follows on from the [Quick Start]({{< relref "driver-reactive/getting-started/quick-start.md" >}}).

- The following import statements:

```java
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
```

- The following POJO classes. The full source is available on github for the [Person]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/Person.java">}}) and [Address]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/Address.java">}})
POJOs. Here are the main implementation details:

```java
public final class Person {
    private ObjectId id;
    private String name;
    private int age;
    private Address address;

    public Person() {
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(final int age) {
        this.age = age;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(final Address address) {
        this.address = address;
    }
    
    // Rest of implementation
}

public final class Address {
    private String street;
    private String city;
    private String zip;

    public Address() {
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(final String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(final String city) {
        this.city = city;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(final String zip) {
        this.zip = zip;
    }
    
    // Rest of implementation
}
```
## Creating a Custom CodecRegistry

Before you can use a POJO with the driver, you need to configure the [`CodecRegistry` ]({{< relref "bson/codecs.md" >}}) to include a codecs 
to handle the translation to and from [`bson`]({{< relref "bson/index.md" >}}) for your POJOs. The simplest way to do that is to use the 
[`PojoCodecProvider.builder()`]({{< apiref "org/bson/codecs/pojo/PojoCodecProvider.html">}}) to create and configure a `CodecProvider`.

The following example will combine the default codec registry, with the `PojoCodecProvider` configured to automatically create POJO 
`Codec`s:

```java
CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
```

### Using the CodecRegistry

There are multiple ways to set the `pojoCodecRegistry` for use:

 - You can set it when instantiating a MongoClient object:
 
 ```java
MongoClientSettings settings = MongoClientSettings.builder()
        .codecRegistry(pojoCodecRegistry)
        .build();
MongoClient mongoClient = MongoClients.create(settings);
```

- You can use an alternative `CodecRegistry` with a `MongoDatabase`:

 ```java
database = database.withCodecRegistry(pojoCodecRegistry);
```

- You can use an alternative `CodecRegistry` with a `MongoCollection`:

 ```java
collection = collection.withCodecRegistry(pojoCodecRegistry);
```

## Inserting a POJO into MongoDB

The codec registry will automatically try to create a POJO `Codec` for unknown classes. This allows you to use POJOs out of the 
box without any extra configuration. See the [Bson POJO page]({{< ref "bson/pojos.md" >}}) for information on configuring POJO `Codec`s.

Before you can insert a POJO into MongoDB, you need a `MongoCollection` instance configured with the Pojo's type:

```java
MongoCollection<Person> collection = database.getCollection("people", Person.class);
```

### Insert a Person

To insert a Person into the collection, you can use the collection's [`insertOne()`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#insertOne-TDocument-" >}}) method.

```java
Person ada = new Person("Ada Byron", 20, new Address("St James Square", "London", "W1"));

collection.insertOne(ada).subscribe(new OperationSubscriber<Success>());
```

### Insert Many Persons

To add multiple Person instances, you can use the collection's [`insertMany()`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#insertMany-java.util.List-" >}}) method 
which takes a list of `Person`.

The following example will add multiple Person instances into the collection:

```java
List<Person> people = asList(
        new Person("Charles Babbage", 45, new Address("5 Devonshire Street", "London", "W11")),
        new Person("Alan Turing", 28, new Address("Bletchley Hall", "Bletchley Park", "MK12")),
        new Person("Timothy Berners-Lee", 61, new Address("Colehill", "Wimborne", null))
);

collection.insertMany(people).subscribe(new OperationSubscriber<Success>());
```

## Query the Collection

To query the collection, you can use the collection's [`find()`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#find--">}}) method. 

The following example prints all the Person instances in the collection:
```java
ObservableSubscriber<Person> subscriber = new PrintToStringSubscriber<>();
collection.find().subscribe(subscriber);
subscriber.await();
```

The example blocks on the `Publisher` until its completed and outputs the following:

```bash
Person{id='591dbc2550852fa685b3ad17', name='Ada Byron', age=20, address=Address{street='St James Square', city='London', zip='W1'}}
Person{id='591dbc2550852fa685b3ad18', name='Charles Babbage', age=45, address=Address{street='5 Devonshire Street', city='London', zip='W11'}}
Person{id='591dbc2550852fa685b3ad19', name='Alan Turing', age=28, address=Address{street='Bletchley Hall', city='Bletchley Park', zip='MK12'}}
Person{id='591dbc2550852fa685b3ad1a', name='Timothy Berners-Lee', age=61, address=Address{street='Colehill', city='Wimborne', zip='null'}}
```

## Specify a Query Filter

To query for Person instance that match certain conditions, pass a filter object to the [`find()`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#find--">}}) method. 
To facilitate creating filter objects, the Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper.

{{% note class="important" %}}
When querying POJOs you *must* query against the document field name and not the Pojo's property name. 
By default they are the same but it is possible to change how POJO property names are mapped.
{{% /note %}}


### Get A Single Person That Matches a Filter

For example, to find the first `Person` in the database that lives in `Wimborne` pass an [`eq`]({{<apiref  "com/mongodb/client/model/Filters.html#eq-java.lang.String-TItem-">}}) 
filter object to specify the equality condition:

```java
personSubscriber = new PrintToStringSubscriber<>();
collection.find(eq("address.city", "Wimborne")).first().subscribe(personSubscriber);
personSubscriber.await();
```
The example prints one document:

```bash
Person{id='591dbc2550852fa685b3ad1a', name='Timothy Berners-Lee', age=61, 
       address=Address{street='Colehill', city='Wimborne', zip='null'}}
```

### Get All Person Instances That Match a Filter

The following example returns and prints everyone where ``"age" > 30``:

```java
personSubscriber = new PrintToStringSubscriber<>();
collection.find(gt("age", 30)).subscribe(personSubscriber);
personSubscriber.await();
```

## Update Documents

To update documents in a collection, you can use the collection's [`updateOne`]({{<apiref "com/mongodb/reactivestreams/client/MongoCollection.html#updateOne-org.bson.conversions.Bson-org.bson.conversions.Bson-">}})  and  [`updateMany`]({{<apiref "com/mongodb/async/client/MongoCollection.html#updateMany-org.bson.conversions.Bson-org.bson.conversions.Bson-">}}) methods.

Pass to the methods:

- A filter object to determine the document or documents to update. To facilitate creating filter objects, the Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper. To specify an empty filter (i.e. match all Persons in a collection), use an empty [`Document`]({{< apiref "org/bson/Document.html" >}}) object.

- An update document that specifies the modifications. For a list of the available operators, see [update operators]({{<docsref "reference/operator/update-field">}}).

The update methods return an [`UpdateResult`]({{<apiref "com/mongodb/client/result/UpdateResult.html">}}) which provides information about the operation including the number of documents modified by the update.

### Update a Single Person

To update at most a single `Person`, use the [`updateOne`]({{<apiref "com/mongodb/reactivestreams/client/MongoCollection.html#updateOne-org.bson.conversions.Bson-org.bson.conversions.Bson-">}}) method.

The following example updates `Ada Byron` setting their age to `23` and name to `Ada Lovelace`:

```java
ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();
collection.updateOne(eq("name", "Ada Byron"), combine(set("age", 23), set("name", "Ada Lovelace")))
        .subscribe(updateSubscriber);
updateSubscriber.await();
```

### Update Multiple Persons

To update all Persons that match a filter, use the [`updateMany`]({{<apiref "com/mongodb/async/client/MongoCollection.html#updateMany-org.bson.conversions.Bson-org.bson.conversions.Bson-">}}) method.

The following example sets the zip field to `null` for all documents that have a `zip` value:

```java
updateSubscriber = new OperationSubscriber<>();
collection.updateMany(not(eq("zip", null)), set("zip", null))
        .subscribe(updateSubscriber);
updateSubscriber.await();
```

### Replace a Single Person

An alternative method to change an existing `Person`, would be to use the [`replaceOne`]({{<apiref "com/mongodb/reactivestreams/client/MongoCollection.html#replaceOne-org.bson.conversions.Bson-TDocument-">}}) method.

The following example replaces the `Ada Lovelace` back to the original document:

```java
updateSubscriber = new OperationSubscriber<>();
collection.replaceOne(eq("name", "Ada Lovelace"), ada).subscribe(updateSubscriber);
updateSubscriber.await();
```

## Delete Documents

To delete documents from a collection, you can use the collection's [`deleteOne`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#deleteOne-org.bson.conversions.Bson-">}}) and [`deleteMany`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#deleteMany-org.bson.conversions.Bson-">}}) methods.

Pass to the methods a filter object to determine the document or documents to delete. To facilitate creating filter objects, the Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "org/bson/Document.html" >}}) object.

The delete methods return a [`DeleteResult`]({{< apiref "com/mongodb/client/result/DeleteResult.html">}})
which provides information about the operation including the number of documents deleted.

### Delete a Single Person That Matches a Filter

To delete at most a single `Person` that matches a filter, use the [`deleteOne`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#deleteOne-org.bson.conversions.Bson-">}}) method:

The following example deletes at most one `Person` who lives in `Wimborne`:

```java
ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();
collection.deleteOne(eq("address.city", "Wimborne")).subscribe(deleteSubscriber);
deleteSubscriber.await();
```

### Delete All Persons That Match a Filter

To delete multiple Persons matching a filter use the [`deleteMany`]({{< apiref "com/mongodb/reactivestreams/client/MongoCollection.html#deleteMany-org.bson.conversions.Bson-">}}) method.

The following example deletes all Persons that live in `London`:

```java
deleteSubscriber = new OperationSubscriber<>();
collection.deleteMany(eq("address.city", "London")).subscribe(deleteSubscriber);
deleteSubscriber.await();
```

### Additional Information

For additional information about the configuring the `PojoCodecProvider`, see the [Bson POJO page]({{< ref "bson/pojos.md" >}}).

For additional tutorials about using MongoDB (such as to use the aggregation framework, specify write concern, etc.), see [Java Driver Tutorials]({{< ref "driver-reactive/tutorials/index.md" >}}).

