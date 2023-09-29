# напишите ваш код ниже
def compare_df(left, right):
    return (set(left.columns) == set(right.columns)) & (
        left.count()
        == right.count()
        == left.unionByName(right).distinct().count()
    )
