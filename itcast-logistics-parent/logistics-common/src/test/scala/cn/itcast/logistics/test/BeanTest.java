package cn.itcast.logistics.test;

import java.util.Objects;

public class BeanTest {

	public BeanTest() {
	}

	public BeanTest(Integer id, String name) {
		this.id = id;
		this.name = name;
	}

	private Integer id ;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	private String name ;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BeanTest beanTest = (BeanTest) o;
		return Objects.equals(id, beanTest.id) &&
			Objects.equals(name, beanTest.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, name);
	}

	@Override
	public String toString() {
		return "BeanTest{" +
			"id=" + id +
			", name='" + name + '\'' +
			'}';
	}
}
