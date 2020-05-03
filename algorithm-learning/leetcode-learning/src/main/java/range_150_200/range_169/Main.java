import java.util.HashMap;
import java.util.Map;

/**
 * 给定一个大小为 n 的数组，找到其中的多数元素。多数元素是指在数组中出现次数大于 ⌊ n/2 ⌋ 的元素。
 *
 * 你可以假设数组是非空的，并且给定的数组总是存在多数元素。
 *
 *  
 *
 * 示例 1:
 *
 * 输入: [3,2,3]
 * 输出: 3
 * 示例 2:
 *
 * 输入: [2,2,1,1,1,2,2]
 * 输出: 2
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode-cn.com/problems/majority-element
 * 著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
 */
public class Main {

    public static void main(String []args) {
        int []nums = {10,9,9,9,10};
        System.out.println(methodThree(nums));
    }


    /**
     * 方法一：暴力求解
     * 遍历整个数组进行累计，若是发现没有超过一半则从下一位元素开始
     *
     * 时间复杂度(n*n)
     * 空间复杂度(1)
     */
    public static int methodOne(int []nums) {
        if (nums == null || nums.length <=0) return -1;
        if (nums.length == 1) return nums[0];

        int count = 0;
        for (int i:nums) {
            for (int j: nums) {
                if (i == j) count++;
                if (count > nums.length/2) return count;
            }
            count = 0;
        }

        return -1;
    }

    /**
     * 方法二：哈希表统计
     *
     * 时间复杂度(n)
     * 空间复杂度(n)
     */
    public static int methodTwo(int []nums) {
        if (nums == null || nums.length <=0) return -1;
        if (nums.length == 1) return nums[0];

        Map<Integer, Integer> map = new HashMap<>(nums.length);
        int maxCount = 0;
        int maxNum = nums[0];
        for (int i: nums) {
            int count = map.getOrDefault(i, 0)+1;
            map.put(i, count);
            if (count > maxCount) {
                maxCount = count;
                maxNum = i;
            }
        }
        return maxNum;
    }


    /**
     * 方法三：摩尔投票法
     * 1. 定义一个condidate记录当前遍历的元素，一个count记录当前数字出现的次数
     * 2. 将condidate设置为数组的第一位元素，count设置为1
     * 3. 遍历数组，如果下一位和condidate一样，则count +1；否则count -1
     * 4. 当count归0时，则condidate设置为当前遍历的那个元素，并将count设置为1
     *
     * 核心思想：相当于超过半数的那个元素去和其他元素一起置换掉
     *          即便怎么换，到最后剩下的都是超过半数的元素
     * 局限性：必须保证元素里面一定有超过一半的元素，否则该方法选出的元素不可控制
     *
     * 时间复杂度(n)
     * 空间复杂度(1)
     */
    public static int methodThree(int []nums) {
        if (nums == null || nums.length <=0) return -1;
        if (nums.length == 1) return nums[0];

        int condidate = nums[0];
        int count = 1;
        for (int i = 1; i < nums.length; i++) {
            if (count == 0){
                condidate = nums[i];
                count = 1;
            } else if (nums[i] == condidate) {
                count++;
            } else {
                count--;
            }
        }
        return condidate;
    }
}
