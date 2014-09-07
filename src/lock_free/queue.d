module lock_free.queue;

import core.atomic;

/**
 *  A cas-based queue
 */
shared class Queue(T)
{
    private struct Node
    {
        this(T val) shared
        {
            this.val = val;
        }

        T val;
        Node* next;
    }

    private void lfence()
    {
        asm
        {
            naked;
            lfence;
            ret;
        }
    }

    private Node* _head, _tail;

    this()
    {
        auto node = new shared(Node)(T.init);
        this._head = node;
        this._tail = node;
    }

    /**
     *  Even if empty is false, this does not guarantee that a call to
     *  pop will succeed.
     *  Returns:
     *      Wheter the queue is empty.
     */
    @property bool empty()
    {
        auto tail = this._tail;
        return this._head == tail && tail.next is null;
    }

    /**
     *  Atomically push a new value to the end of the queue.
     */
    void push(T val)
    {
        auto node = new shared(Node)(val);
        shared(Node)* tail, next;
        while (true)
        {
            tail = this._tail;
            lfence();
            next = tail.next;
            if (tail == this._tail)
            {
                if (next is null)
                {
                    if (cas(&tail.next, next, node))
                        break;
                }
                else
                {
                    cas(&this._tail, tail, next);
                }
            }
        }
        cas(&this._tail, tail, node);
    }

    /**
     *  Atomically pop a value from the front of the queue.
     *  Returns:
     *      null if the queue is empty
     */
    shared(T)* pop()
    {
        shared(Node)* head, tail, next;
        while (true)
        {
            head = this._head;
            lfence();
            tail = this._tail;
            next = head.next;
            if (head == this._head)
            {
                if (head == tail)
                {
                    if (next is null)
                        return null;
                    else
                        cas(&this._tail, tail, next);
                }
                else
                {
                    if (cas(&this._head, head, next))
                        break;
                }
            }
        }
        return &next.val;
    }
}

unittest
{
    enum amount = 100_000;
    auto queue = new shared(Queue!int);
    import std.stdio: writeln;
    import std.datetime: StopWatch, AutoStart, TickDuration;

    auto sw = StopWatch();
    sw.reset;
    sw.start();

    foreach (i; 0 .. amount)
        queue.push(i);
    foreach (i; 0 .. amount)
        assert(*queue.pop() == i);

    sw.stop;

    writeln("Duration: ", sw.peek.usecs, " microseconds");
    writeln("Framerate: ", 1e6/sw.peek.usecs, " frames per second");
}
