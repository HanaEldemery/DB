package DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different,
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements java.io.Serializable {
	private final String indexName;
	private BTreeNode<TKey> root;

	public BTree(String indexName) {
		this.root = new BTreeLeafNode<TKey, TValue>();
		this.indexName = indexName;
	}

	public int getRootKeyCount(){
		return root.keyCount;
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		//Handle Duplicates
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		if(search(key) != null){
			leaf.insertKey(key, value);
			return;
		}
		leaf.insertKey(key, value);
		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public Vector<TValue> search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}

	/**
	 * Delete a key and its associated value from the tree.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n;
		}
	}

	public void delete(TKey key, TValue value){ 
		BTreeLeafNode<TKey, TValue> leaf = this.getLeafNodeForMinVal(key);
		boolean flag = false;
		while(!flag && leaf != null){
			flag = leaf.delete(key,value);

			if (leaf.isUnderflow()) {
				BTreeNode<TKey> n = leaf.dealUnderflow();
				if (n != null)
					this.root = n;
			}
			leaf = leaf.getRightSibling();
		}

	}

	/**
	 * Get the range of values in this B+ tree that are between this min and max
	 */

	public LinkedList<Pointer<TKey,TValue>> searchByRange(TKey min, TKey max){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> minNode = this.getLeafNodeForMinVal(min);
		// Keep going to the right sibling and add all the values that fit the range into a linkedList
		do{
			for (int i = 0; i < minNode.getKeyCount(); i++) {
				if(minNode.getKey(i).compareTo(min) >= 0 && minNode.getKey(i).compareTo(max) <= 0){
					for (int j = 0; j < minNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(minNode.getKey(i),minNode.getValue(i).get(j)));
					}
				}

				if(minNode.getKey(i).compareTo(min) < 0 && minNode.getKey(i).compareTo(max) >= 0){
					return list;
				}
			}
			minNode = minNode.getRightSibling();
		}while (minNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getLessThanKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(((Comparable)currentNode.getKey(i)).compareTo(""+key) < 0){
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i).get(j)));
					}
				}else {
					return list;
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getLessThanOrEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(((Comparable)currentNode.getKey(i)).compareTo(""+key) <= 0){
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i).get(j)));
					}
				}else {
					return list;
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getMoreThanKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeForMinVal(key);
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(((Comparable)currentNode.getKey(i)).compareTo(""+key) == 0){
					continue;
				}
				if(((Comparable)currentNode.getKey(i)).compareTo(""+key) > 0){
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i).get(j)));
					}
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getMoreThanOrEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeForMinVal(key);
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(((Comparable)currentNode.getKey(i)).compareTo((Comparable)(""+key)) >= 0){
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i).get(j)));
					}
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getNotEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(((Comparable)currentNode.getKey(i)).compareTo(""+key) != 0){
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i).get(j)));
					}
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		////////////////////////////////////////////////////////////
		TKey key2=(TKey)(""+key);
		Vector<TValue> vector = search(key2);
		if (vector==null){
			return list;
		}
		////////////////////////////////////////////////////////////
		//Vector<TValue> vector = search(key);
		for (TValue value : vector) {
			list.add(new Pointer<>(key, value));
		}
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> computeOperator(TKey key, String operator){
		return switch (operator) {
			case "<" -> getLessThanKeys(key);
			case "<=" -> getLessThanOrEqualKeys(key);
			case ">" -> getMoreThanKeys(key);
			case ">=" -> getMoreThanOrEqualKeys(key);
			case "!=" -> getNotEqualKeys(key);
			case "=" -> getEqualKeys(key);
			default -> new LinkedList<>();
		};
	}

	public int getPageNumberForInsert(TKey primaryKey){
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeBeforeKey(primaryKey);
		int pageNumber = 0;
		if(currentNode != null && currentNode.getRightSibling() == null && currentNode.getKey(currentNode.getKeyCount()-1).compareTo(primaryKey)<0){
			return Integer.parseInt(((Vector<String>)currentNode.getValue(currentNode.getKeyCount()-1)).get(0).split("-")[0]);
		}
		while (currentNode!=null){
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(primaryKey) < 0){
					pageNumber = Integer.parseInt(((Vector<String>)currentNode.getValue(i)).get(0).split("-")[0]);
				} else if(currentNode.getKey(i).compareTo(primaryKey) == 0){
					return -1;
				}else {
					return pageNumber;
				}
			}
			currentNode = currentNode.getRightSibling();
		}
		return 1;
	}

	public void reducePageNumbers(int pageNumber){
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				for (int j = 0; j < currentNode.getValue(i).size(); j++) {
					int num = Integer.parseInt(currentNode.getValue(i).get(j).toString().split("-")[0]);
					String primaryKey = currentNode.getValue(i).get(j).toString().split("-")[1];
					String tableName = currentNode.getValue(i).get(j).toString().split("-")[2];
					if(num >= pageNumber){
						currentNode.setValueInVector(i, j,(TValue) ((num-1)+"-"+primaryKey+"-"+tableName));
					}
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
	}

	private BTreeLeafNode<TKey, TValue> getLeafNodeBeforeKey(TKey key) {
		BTreeNode<TKey> currentNode = this.root;
		BTreeLeafNode<TKey, TValue> prevLeafNode = null;

		while (currentNode instanceof BTreeInnerNode<TKey> innerNode) {
			int childIndex = innerNode.getChildIndex(key);
			if (childIndex == -1) {
				// Key is smaller than all children, follow the leftmost child
				currentNode = innerNode.getChild(0);
			} else {
				// Key is greater than or equal to the child at the specified index
				currentNode = innerNode.getChild(childIndex+1);
				if (!(currentNode instanceof BTreeInnerNode)) {
					prevLeafNode = (BTreeLeafNode<TKey, TValue>) currentNode;
				}
			}
		}
		return prevLeafNode;
	}


	/**
	 * Find the leaf node that contains the specified key.
	 */
	private BTreeLeafNode<TKey, TValue> getLeafNodeForMinVal(TKey key) {
		BTreeNode<TKey> currentNode = this.root;

		while (currentNode instanceof BTreeInnerNode<TKey> innerNode) {
			int childIndex = innerNode.getChildIndex(key);

			if (childIndex == -1) {
				// Key is smaller than all children, follow the leftmost child
				currentNode = innerNode.getChild(0);
			} else {
				// Key is greater than or equal to the child at the specified index
				currentNode = innerNode.getChild(childIndex);
			}
		}
		return (BTreeLeafNode<TKey, TValue>) currentNode;
	}

	public BTreeLeafNode<TKey, TValue> getFirstLeafNodeOnLeft() {
		BTreeNode<TKey> currentNode = this.root;

		// Traverse towards the leftmost leaf node
		while (currentNode instanceof BTreeInnerNode<TKey>) {
			currentNode = ((BTreeInnerNode<TKey>) currentNode).getChild(0); // Follow the leftmost child
		}

		return (BTreeLeafNode<TKey, TValue>) currentNode;
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}

		return (BTreeLeafNode<TKey, TValue>)node;
	}

	public boolean checkKeyExists(TKey key){
		return search(key) != null;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreeNode<TKey>> queue = new LinkedList<>();
		queue.offer(root);

		while (!queue.isEmpty()) {
			int levelSize = queue.size();

			for (int i = 0; i < levelSize; i++) {
				BTreeNode<TKey> node = queue.poll();

				// Append node with a border
				assert node != null;
				sb.append("|").append(node.toString().trim());

				if (node.getNodeType() == TreeNodeType.InnerNode) {
					BTreeInnerNode<TKey> innerNode = (BTreeInnerNode<TKey>) node;
					for (int j = 0; j <= innerNode.getKeyCount(); j++) {
						BTreeNode<TKey> child = innerNode.getChild(j);
						if (child != null) {
							queue.offer(child);
						}
					}
				}
			}
			sb.append("|\n");
		}
		String[] strings = sb.toString().split("\n");
		StringBuilder result = new StringBuilder();
		for(int i = 0; i<strings.length;i++){
			strings[i] = (" ").repeat((strings[strings.length-1].length()-strings[i].length())/2) + strings[i];
			strings[i] = "Level " + i + ": " + strings[i];
			result.append(strings[i]).append("\n");
		}

		return result.toString();
	}

	public static void serialize(BTree t) throws IOException {
        try {
            String s = t.indexName + ".ser";
            File theDir = new File("Trees/");
            if (!theDir.exists()){
                theDir.mkdirs();
            }
            FileOutputStream fos = new FileOutputStream("Trees/" + s);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(t);

            fos.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

	public static BTree deserialize(String filename) throws IOException, ClassNotFoundException {
        BTree x = null;
        try{
            FileInputStream fis = new FileInputStream("Trees/" + filename + ".ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            x = (BTree) ois.readObject();
            ois.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return x;
    }

	public static void main(String[] args){
		BTree<String,String> bTree = new BTree<String,String>("gpaIndex");
		//123 is the value in the table while 5,12 is the pointer to the 5th page in the 12th index
		//insert(key,value)

		bTree.insert("hana","0-1");
		bTree.insert("omar","0-2");
		bTree.insert("adham","1-3");
		bTree.insert("hana","1-4");
		bTree.insert("hana","1-5");
		bTree.insert("hana","5-6");
		/*bTree.insert("10","8-12");
		bTree.insert("11","5-13");
		bTree.insert("12","5-14");
		bTree.insert("13","5-15");
		bTree.insert("14","5-16");
		bTree.insert("15","5-12");
		bTree.insert("16","5-12");
		bTree.insert("17","5-12");
		bTree.insert("18","5-12");
		bTree.insert("19","5-12");
		bTree.insert("20","5-12");
		bTree.insert("21","5-12");
		bTree.insert("22","5-12");
		bTree.insert("23","5-12");
		bTree.insert("11.5","5-12");*/
		//String data = String.valueOf(bTree.search("hana"));
		//System.out.println(data);
		//data = String.valueOf(bTree.search("10"));
		//System.out.println(data);

		System.out.println(bTree);
		//bTree.delete(null);
		//System.out.println("bb"+bTree);
		System.out.println(bTree.search("hana"));
		Vector<String> testing= new Vector<String>();
		testing.add("111-9");
		String thePage= testing.get(0).split("-")[0];
		System.out.println("the page "+thePage);
		//LinkedList<Pointer<String, String>> list = bTree.searchByRange("1", "14");
		//for (Pointer<String,String> pointer : list) {
			//System.out.println(pointer.key() + ", " + pointer.value());
		//}

		//bTree.delete("hana","1-4");
		//System.out.println(bTree);
		//System.out.println(bTree);
	}
}

