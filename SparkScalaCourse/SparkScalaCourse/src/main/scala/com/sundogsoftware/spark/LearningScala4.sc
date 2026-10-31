// Data structures

// A tuple groups a fixed number of values, which may have different types.

val captainStuff = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

// Tuple accessors are numbered from 1, unlike list/array indexes.
println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

val picardsShip = "Picard" -> "Enterprise-D"
println(picardsShip._2)

val aBunchOfStuff = ("Kirk", 1964, true)

// A List is an ordered collection whose elements share a type. List is
// immutable: operations below return new lists rather than changing shipList.

val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Space Nine")

println(shipList(1))
// zero-based

println(shipList.head)
println(shipList.tail)

for (ship <- shipList) {println(ship)}

val backwardShips = shipList.map( (ship: String) => {ship.reverse})
for (ship <- backwardShips) {println(ship)}

// reduce combines the collection into one value. It needs a non-empty list
// and a combining operation that can be applied repeatedly.
val numberList = List(1, 2, 3, 4,5 )
val sum = numberList.reduce( (x: Int, y: Int) => x + y)
println(sum)

// filter returns a new collection containing only elements that pass a test.
val iHateFives = numberList.filter( (x: Int) => x != 5)

val iHateThrees = numberList.filter(_ != 3)

// Concatenate lists
val moreNumbers = List(6,7,8)
val lotsOfNumbers = numberList ++ moreNumbers

val reversed = numberList.reverse
val sorted = reversed.sorted
val lotsOfDuplicates = numberList ++ numberList
val distinctValues = lotsOfDuplicates.distinct
val maxValue = numberList.max
val total = numberList.sum
val hasThree = iHateThrees.contains(3)

// A Map stores key -> value associations. Direct lookup throws if a key is
// absent; Try(...).getOrElse(...) demonstrates one way to provide a fallback.
val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway" -> "Voyager")
println(shipMap("Janeway"))
println(shipMap.contains("Archer"))
val archersShip = util.Try(shipMap("Archer")) getOrElse "Unknown"
println(archersShip)

// EXERCISE
// Create a list of the numbers 1-20; your job is to print out numbers that are evenly divisible by three. (Scala's
// modula operator, like other languages, is %, which gives you the remainder after division. For example, 9 % 3 = 0
// because 9 is evenly divisible by 3.) Do this first by iterating through all the items in the list and testing each
// one as you go. Then, do it again by using a filter function on the list instead.
