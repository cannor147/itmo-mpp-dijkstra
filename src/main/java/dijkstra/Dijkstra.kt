package dijkstra

import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import kotlin.Comparator
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    start.distance = 0
    val q = MulitQueue(workers)
    q.add(start)
    val activeNodes = AtomicInteger(1)
    val onFinish = Phaser(workers + 1)
    repeat(workers) {
        thread {
            while (true) {
                val cur = q.poll()
                if (cur == null) {
                    if (activeNodes.get() == 0) {
                        break
                    } else {
                        continue
                    }
                }
                for (e in cur.outgoingEdges) {
                    while (true) {
                        val currentDistance = e.to.distance
                        val x = cur.distance + e.weight
                        if (currentDistance > x) {
                            if (e.to.casDistance(currentDistance, x)) {
                                q.add(e.to)
                                activeNodes.incrementAndGet()
                                break
                            }
                        } else {
                            break
                        }
                    }
                }
                activeNodes.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

class MulitQueue(private val workers: Int) {
    private val queues = ArrayList<Queue<Node>>(workers)

    init {
        for (i in 1..workers) {
            queues.add(PriorityQueue(NODE_DISTANCE_COMPARATOR))
        }
    }

    fun add(node: Node) {
        val index = ThreadLocalRandom.current().nextInt(workers)
        val q = queues[index]

        synchronized(q) {
            q.add(node)
        }
    }

    fun poll(): Node? {
        while (true) {
            val firstIndex = ThreadLocalRandom.current().nextInt(workers)
            val secondIndex = ThreadLocalRandom.current().nextInt(workers)
            if (firstIndex == secondIndex) {
                continue
            }

            val q1 = queues[minOf(firstIndex, secondIndex)]
            val q2 = queues[maxOf(firstIndex, secondIndex)]

            synchronized(q1) {
                synchronized(q2) {
                    val x1 = q1.peek()
                    val x2 = q2.peek()
                    return when {
                        x1 == null && x2 == null -> null
                        x1 != null && x2 == null -> q1.poll()
                        x1 == null && x2 != null -> q2.poll()
                        NODE_DISTANCE_COMPARATOR.compare(x1, x2) < 0 -> q1.poll()
                        else -> q2.poll()
                    }
                }
            }
        }
    }
}