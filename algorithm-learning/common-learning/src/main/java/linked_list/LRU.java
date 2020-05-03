package linked_list;

/**
 * @Author:linchonghui
 * @Date:1/5/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */

/**
 *  遍历链表，判断是否已缓存当前数据
 *  1. 如果发现链表中已经存在需要缓存的节点，则删除该节点，并将新增的节点添加到链表头节点
 *  2. 如果没有存在数据，则判断链表是否已经满
 *     ① 如果满了，则删除链表尾节点，将新增的节点添加到链表头节点
 *     ② 直接将新增的节点添加到链表头节点
 */
public class LRU {

    private static ListNode head;
    private static final Integer maxLength = 5;

    public void main(String []args) {

    }

    public void setLRU(ListNode head, ListNode newListNode) {
        int listLength = 0;
        while (head.next != null) {

        }

    }

}


class ListNode {
      int val;
      ListNode next;
      ListNode(int x) { val = x; }
}