package mongodao

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// User 用户（MongoDB trade.user 集合）。
type User struct {
	ID          int64  `bson:"user_id"`
	LegacyID    int64  `bson:"id,omitempty"`
	Username    string `bson:"username"`
	Password    string `bson:"password"`
	PhoneNumber int64  `bson:"phone_number,omitempty"`
	Status      int32  `bson:"status"`
	CreatedAt   int64  `bson:"created_at,omitempty"`
	UpdatedAt   int64  `bson:"updated_at,omitempty"`
}

func (u *User) normalizeID() {
	if u.ID == 0 && u.LegacyID > 0 {
		u.ID = u.LegacyID
	}
}

type UserRepo struct {
	coll *mongo.Collection
}

func NewUserRepo(coll *mongo.Collection) *UserRepo {
	return &UserRepo{coll: coll}
}

func (r *UserRepo) EnsureIndex(ctx context.Context) error {
	_, err := r.coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "username", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "user_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	})
	return err
}

func (r *UserRepo) FindByUsername(ctx context.Context, username string) (*User, error) {
	var u User
	err := r.coll.FindOne(context.Background(), bson.M{"username": username}).Decode(&u)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	u.normalizeID()
	return &u, nil
}

func (r *UserRepo) FindByID(ctx context.Context, userID int64) (*User, error) {
	var u User
	err := r.coll.FindOne(ctx, bson.M{"$or": bson.A{
		bson.M{"user_id": userID},
		bson.M{"id": userID},
	}}).Decode(&u)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	u.normalizeID()
	return &u, nil
}
